-- ============================================================================
-- ENG-1973: Migration Metadata Infrastructure
-- ============================================================================

DROP TABLE IF EXISTS derived.eng1973_migration_metadata CASCADE;

CREATE TABLE derived.eng1973_migration_metadata (
  id BIGINT IDENTITY(1,1) PRIMARY KEY,
  migration_step VARCHAR(255) NOT NULL,
  records_affected BIGINT,
  status VARCHAR(50) NOT NULL,
  notes VARCHAR(65535),
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
DISTSTYLE AUTO
SORTKEY (created_at);

COMMENT ON TABLE derived.eng1973_migration_metadata IS
'Tracks execution status and metrics for ENG-1973 email enhancement migration steps.';

GRANT SELECT ON derived.eng1973_migration_metadata TO reporting_role;
GRANT INSERT, UPDATE ON derived.eng1973_migration_metadata TO migration_role;

INSERT INTO derived.eng1973_migration_metadata
(migration_step, records_affected, status, notes)
VALUES (
  'metadata_table_created',
  0,
  'success',
  'Email enhancement tracking infrastructure established'
);
