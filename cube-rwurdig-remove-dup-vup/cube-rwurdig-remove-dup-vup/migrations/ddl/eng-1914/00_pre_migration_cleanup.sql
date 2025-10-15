-- ============================================================================
-- ENG-1914 Phase 0: Pre-Migration Cleanup of Orphaned Child Records
-- Purpose: Establish clean referential integrity before deduplication begins
-- Expected Impact: Remove ~3.7M orphaned jobs and ~34.2M orphaned emails
-- Prerequisites: Database backup completed, maintenance window scheduled
-- ============================================================================

-- Create audit log table for tracking cleanup operations
CREATE TABLE IF NOT EXISTS derived.eng1914_cleanup_log (
    cleanup_id VARCHAR(32) DEFAULT MD5(RANDOM()::TEXT || CLOCK_TIMESTAMP()::TEXT),
    checkpoint VARCHAR(100) NOT NULL,
    orphaned_jobs BIGINT,
    orphaned_emails BIGINT,
    total_persons BIGINT,
    recorded_at TIMESTAMP DEFAULT GETDATE(),
    PRIMARY KEY (cleanup_id)
);

COMMENT ON TABLE derived.eng1914_cleanup_log IS 
'Audit trail for ENG-1914 pre-migration cleanup operations tracking orphaned record removal';

-- Record baseline orphan counts before any cleanup
INSERT INTO derived.eng1914_cleanup_log (
    checkpoint, 
    orphaned_jobs, 
    orphaned_emails, 
    total_persons
)
SELECT 
    'Pre-Cleanup Baseline',
    (SELECT COUNT(*) 
     FROM derived.vector_universal_job vuj 
     LEFT JOIN derived.vector_universal_person vup ON vuj.vup_id = vup.vup_id
     WHERE vup.vup_id IS NULL),
    (SELECT COUNT(*) 
     FROM derived.vector_email ve 
     LEFT JOIN derived.vector_universal_person vup ON ve.vup_id = vup.vup_id
     WHERE vup.vup_id IS NULL AND ve.vup_id IS NOT NULL),
    (SELECT COUNT(*) FROM derived.vector_universal_person);

-- Display current baseline for review
SELECT 
    'Baseline Orphan Analysis' as analysis_type,
    checkpoint,
    total_persons as total_person_records,
    orphaned_jobs,
    orphaned_emails,
    ROUND(100.0 * orphaned_jobs / NULLIF(total_persons, 0), 2) as pct_orphaned_jobs,
    ROUND(100.0 * orphaned_emails / NULLIF(total_persons, 0), 2) as pct_orphaned_emails,
    recorded_at
FROM derived.eng1914_cleanup_log
WHERE checkpoint = 'Pre-Cleanup Baseline'
ORDER BY recorded_at DESC
LIMIT 1;

-- Analyze orphan patterns by data source for jobs
SELECT 
    'Orphaned Jobs by Data Source' as analysis_type,
    COALESCE(data_source, 'NULL') as data_source,
    COUNT(*) as orphan_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as pct_of_orphans,
    MIN(created_at) as earliest_created,
    MAX(created_at) as latest_created
FROM derived.vector_universal_job vuj
LEFT JOIN derived.vector_universal_person vup ON vuj.vup_id = vup.vup_id
WHERE vup.vup_id IS NULL
GROUP BY data_source
ORDER BY COUNT(*) DESC
LIMIT 20;

-- Analyze orphan patterns by data source for emails
SELECT 
    'Orphaned Emails by Data Source' as analysis_type,
    COALESCE(data_source, 'NULL') as data_source,
    COUNT(*) as orphan_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as pct_of_orphans,
    COUNT(DISTINCT sha256) as unique_hems
FROM derived.vector_email ve
LEFT JOIN derived.vector_universal_person vup ON ve.vup_id = vup.vup_id
WHERE vup.vup_id IS NULL AND ve.vup_id IS NOT NULL
GROUP BY data_source
ORDER BY COUNT(*) DESC
LIMIT 20;

-- Sample orphaned vup_id values for investigation
SELECT 
    'Sample Orphaned VUP IDs' as analysis_type,
    vup_id as orphaned_vup_id,
    'job' as record_type,
    data_source
FROM derived.vector_universal_job
WHERE vup_id NOT IN (SELECT vup_id FROM derived.vector_universal_person)
LIMIT 10

UNION ALL

SELECT 
    'Sample Orphaned VUP IDs' as analysis_type,
    vup_id as orphaned_vup_id,
    'email' as record_type,
    data_source
FROM derived.vector_email
WHERE vup_id IS NOT NULL 
  AND vup_id NOT IN (SELECT vup_id FROM derived.vector_universal_person)
LIMIT 10;

-- ============================================================================
-- DECISION CHECKPOINT: Review analysis results above
-- If orphan patterns suggest recoverable data, investigate before deletion
-- If patterns indicate data quality issues, proceed with cleanup transaction
-- Uncomment the transaction block below only after review
-- ============================================================================

/*
BEGIN TRANSACTION;

-- Create temporary backup of orphaned records for potential recovery
CREATE TEMP TABLE orphaned_jobs_backup AS
SELECT vuj.*
FROM derived.vector_universal_job vuj
LEFT JOIN derived.vector_universal_person vup ON vuj.vup_id = vup.vup_id
WHERE vup.vup_id IS NULL;

CREATE TEMP TABLE orphaned_emails_backup AS
SELECT ve.*
FROM derived.vector_email ve
LEFT JOIN derived.vector_universal_person vup ON ve.vup_id = vup.vup_id
WHERE vup.vup_id IS NULL AND ve.vup_id IS NOT NULL;

-- Log backup creation
SELECT 
    'Orphaned Records Backed Up' as backup_status,
    (SELECT COUNT(*) FROM orphaned_jobs_backup) as jobs_backed_up,
    (SELECT COUNT(*) FROM orphaned_emails_backup) as emails_backed_up,
    'Temporary tables created for session duration' as note;

-- Delete orphaned job records
DELETE FROM derived.vector_universal_job
WHERE vup_id NOT IN (SELECT vup_id FROM derived.vector_universal_person);

-- Log intermediate state after job cleanup
INSERT INTO derived.eng1914_cleanup_log (
    checkpoint, 
    orphaned_jobs, 
    orphaned_emails, 
    total_persons
)
SELECT 
    'After Job Cleanup',
    (SELECT COUNT(*) 
     FROM derived.vector_universal_job vuj 
     LEFT JOIN derived.vector_universal_person vup ON vuj.vup_id = vup.vup_id
     WHERE vup.vup_id IS NULL),
    (SELECT COUNT(*) 
     FROM derived.vector_email ve 
     LEFT JOIN derived.vector_universal_person vup ON ve.vup_id = vup.vup_id
     WHERE vup.vup_id IS NULL AND ve.vup_id IS NOT NULL),
    (SELECT COUNT(*) FROM derived.vector_universal_person);

-- Delete orphaned email records
DELETE FROM derived.vector_email
WHERE vup_id IS NOT NULL 
  AND vup_id NOT IN (SELECT vup_id FROM derived.vector_universal_person);

COMMIT;
*/

-- Post-cleanup verification (run after uncommenting and executing transaction)
INSERT INTO derived.eng1914_cleanup_log (
    checkpoint, 
    orphaned_jobs, 
    orphaned_emails, 
    total_persons
)
SELECT 
    'Post-Cleanup Verification',
    (SELECT COUNT(*) 
     FROM derived.vector_universal_job vuj 
     LEFT JOIN derived.vector_universal_person vup ON vuj.vup_id = vup.vup_id
     WHERE vup.vup_id IS NULL),
    (SELECT COUNT(*) 
     FROM derived.vector_email ve 
     LEFT JOIN derived.vector_universal_person vup ON ve.vup_id = vup.vup_id
     WHERE vup.vup_id IS NULL AND ve.vup_id IS NOT NULL),
    (SELECT COUNT(*) FROM derived.vector_universal_person);

-- Generate cleanup summary report
SELECT 
    'Cleanup Operation Summary' as report_type,
    baseline.total_persons as total_persons,
    baseline.orphaned_jobs as jobs_before_cleanup,
    final.orphaned_jobs as jobs_after_cleanup,
    baseline.orphaned_jobs - final.orphaned_jobs as jobs_deleted,
    baseline.orphaned_emails as emails_before_cleanup,
    final.orphaned_emails as emails_after_cleanup,
    baseline.orphaned_emails - final.orphaned_emails as emails_deleted,
    CASE 
        WHEN final.orphaned_jobs = 0 AND final.orphaned_emails = 0 
        THEN 'SUCCESS: All orphaned records removed'
        WHEN final.orphaned_jobs < baseline.orphaned_jobs * 0.01 
         AND final.orphaned_emails < baseline.orphaned_emails * 0.01
        THEN 'PARTIAL SUCCESS: Less than 1% orphans remain'
        ELSE 'INCOMPLETE: Significant orphans remain - review required'
    END as cleanup_status,
    CASE 
        WHEN final.orphaned_jobs = 0 AND final.orphaned_emails = 0 
        THEN 'Ready to proceed with Part 1'
        ELSE 'Address remaining orphans before proceeding'
    END as next_action
FROM 
    (SELECT * FROM derived.eng1914_cleanup_log 
     WHERE checkpoint = 'Pre-Cleanup Baseline' 
     ORDER BY recorded_at DESC LIMIT 1) baseline
CROSS JOIN 
    (SELECT * FROM derived.eng1914_cleanup_log 
     WHERE checkpoint = 'Post-Cleanup Verification' 
     ORDER BY recorded_at DESC LIMIT 1) final;

-- Final readiness check
SELECT 
    'Migration Readiness Assessment' as assessment_type,
    CASE 
        WHEN (SELECT orphaned_jobs FROM derived.eng1914_cleanup_log 
              WHERE checkpoint = 'Post-Cleanup Verification' 
              ORDER BY recorded_at DESC LIMIT 1) = 0
         AND (SELECT orphaned_emails FROM derived.eng1914_cleanup_log 
              WHERE checkpoint = 'Post-Cleanup Verification' 
              ORDER BY recorded_at DESC LIMIT 1) = 0
        THEN 'READY FOR MIGRATION'
        ELSE 'NOT READY - ORPHANS PRESENT'
    END as readiness_status,
    'Proceed to Part 1: Backup and Create Infrastructure' as next_phase;
