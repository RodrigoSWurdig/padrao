-- ============================================================================
-- ENG-1914 Phase 0: Pre-Migration Cleanup
-- ============================================================================
-- CRITICAL: Emails preserved via UPDATE (not DELETE) per Andrew
-- ============================================================================

INSERT INTO derived.eng1914_migration_metadata
(migration_step, records_affected, status, notes, created_at)
VALUES (
  'pre_migration_cleanup_start',
  0,
  'in_progress',
  'Beginning cleanup with email preservation strategy',
  CURRENT_TIMESTAMP
);

-- Orphaned email source distribution analysis
SELECT
  'Orphaned Email Source Distribution' AS analysis_type,
  COALESCE(ve.data_source, 'unknown') AS data_source,
  COUNT(*) AS orphaned_count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM derived.vector_email ve
LEFT JOIN derived.vector_universal_person vup ON ve.vup_id = vup.vup_id
WHERE ve.vup_id IS NOT NULL AND vup.vup_id IS NULL
GROUP BY ve.data_source
ORDER BY orphaned_count DESC;

-- Preserve orphaned emails by detaching person reference
UPDATE derived.vector_email
SET vup_id = NULL, updated_at = CURRENT_TIMESTAMP
WHERE vup_id IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM derived.vector_universal_person p 
    WHERE p.vup_id = vector_email.vup_id
  );

INSERT INTO derived.eng1914_migration_metadata
(migration_step, records_affected, status, notes)
VALUES ('email_cleanup_detach', @@ROWCOUNT, 'success', 
        'Detached orphaned emails preserving HEM-to-email resolution capability');

-- Delete orphaned jobs (no preservation value)
DELETE FROM derived.vector_universal_job
WHERE NOT EXISTS (
  SELECT 1 FROM derived.vector_universal_person p 
    WHERE p.vup_id = vector_universal_job.vup_id
);

INSERT INTO derived.eng1914_migration_metadata
(migration_step, records_affected, status, notes)
VALUES ('job_cleanup_delete', @@ROWCOUNT, 'success', 
        'Deleted orphaned jobs per Andrew confirmation');
