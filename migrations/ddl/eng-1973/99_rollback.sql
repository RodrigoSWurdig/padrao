-- ============================================================================
-- ENG-1973 Part 99: Rollback Procedures
-- Purpose: Restore original vector_email table if migration needs to be reverted
-- WARNING: This will delete the new schema and restore the backup
-- ============================================================================

-- ============================================================================
-- ROLLBACK STEP 1: Verify backup exists and is complete
-- ============================================================================
SELECT 
    'Rollback Pre-Check: Backup verification' as check_name,
    (SELECT COUNT(*) FROM derived.vector_email_backup_eng1973) as backup_record_count,
    (SELECT MIN(backup_timestamp) FROM derived.vector_email_backup_eng1973) as backup_created_at,
    CASE 
        WHEN (SELECT COUNT(*) FROM derived.vector_email_backup_eng1973) > 0
        THEN '✓ Backup exists and can be restored'
        ELSE '✗ CRITICAL ERROR: Backup is missing or empty - DO NOT PROCEED'
    END as status;

-- Display backup metadata
SELECT 
    migration_step,
    execution_timestamp,
    records_affected,
    notes
FROM derived.vector_email_migration_metadata_eng1973
ORDER BY execution_timestamp;

-- ============================================================================
-- ROLLBACK STEP 2: Drop new tables and views
-- ============================================================================

-- Drop DynamoDB export view
DROP VIEW IF EXISTS derived.v_dynamodb_email_export CASCADE;

-- Drop temp_vector_emails view (will be recreated pointing to original table)
DROP VIEW IF EXISTS derived.temp_vector_emails CASCADE;

-- Drop new vector_email table
DROP TABLE IF EXISTS derived.vector_email_new CASCADE;

-- Drop domains reference table
DROP TABLE IF EXISTS derived.vector_email_domains CASCADE;

-- ============================================================================
-- ROLLBACK STEP 3: Verify current vector_email table state
-- ============================================================================
SELECT 
    'Rollback Check: Current vector_email state' as check_name,
    COUNT(*) as current_record_count,
    (SELECT COUNT(*) FROM derived.vector_email_backup_eng1973) as backup_record_count,
    CASE 
        WHEN COUNT(*) = (SELECT COUNT(*) FROM derived.vector_email_backup_eng1973)
        THEN 'Vector_email matches backup - no restore needed'
        ELSE 'Vector_email differs from backup - restore needed'
    END as status
FROM derived.vector_email;

-- ============================================================================
-- ROLLBACK STEP 4A: If vector_email was replaced, restore from backup
-- ============================================================================
-- CAUTION: Only execute this if vector_email was replaced with new schema
-- If vector_email still contains original data, skip to Step 5

-- BEGIN TRANSACTION;

-- Drop current vector_email table
-- DROP TABLE IF EXISTS derived.vector_email CASCADE;

-- Restore from backup (recreate original table structure)
-- CREATE TABLE derived.vector_email
-- DISTSTYLE KEY
-- DISTKEY(sha256)
-- SORTKEY(vup_id, sha256)
-- AS
-- SELECT 
--     vup_id,
--     sha256,
--     email,
--     email_type,
--     data_source,
--     dataset_version
-- FROM derived.vector_email_backup_eng1973;

-- Verify restoration
-- SELECT 
--     'Rollback Verification: Table restoration' as check_name,
--     (SELECT COUNT(*) FROM derived.vector_email) as restored_record_count,
--     (SELECT COUNT(*) FROM derived.vector_email_backup_eng1973) as backup_record_count,
--     CASE 
--         WHEN (SELECT COUNT(*) FROM derived.vector_email) = (SELECT COUNT(*) FROM derived.vector_email_backup_eng1973)
--         THEN '✓ Table restored successfully'
--         ELSE '✗ ERROR: Record count mismatch'
--     END as status;

-- COMMIT;

-- ============================================================================
-- ROLLBACK STEP 4B: Alternative - If vector_email wasn't replaced
-- ============================================================================
-- If the migration was stopped before the table swap, vector_email still
-- contains original data. In this case, just verify and skip restoration.

SELECT 
    'Alternative Rollback Path: Original table intact' as check_name,
    COUNT(*) as current_record_count,
    COUNT(DISTINCT sha256) as unique_hems,
    COUNT(DISTINCT vup_id) as unique_vups
FROM derived.vector_email;

-- ============================================================================
-- ROLLBACK STEP 5: Recreate original temp_vector_emails view
-- ============================================================================

CREATE VIEW derived.temp_vector_emails AS
SELECT 
    vup_id,
    sha256,
    email,
    email_type,
    data_source,
    dataset_version
FROM derived.vector_email;

-- Verify view restoration
SELECT 
    'Rollback Verification: View restoration' as check_name,
    COUNT(*) as view_record_count,
    (SELECT COUNT(*) FROM derived.vector_email) as table_record_count,
    CASE 
        WHEN COUNT(*) = (SELECT COUNT(*) FROM derived.vector_email)
        THEN '✓ View restored successfully'
        ELSE '✗ ERROR: View/table mismatch'
    END as status
FROM derived.temp_vector_emails;

-- ============================================================================
-- ROLLBACK STEP 6: Record rollback in metadata
-- ============================================================================

INSERT INTO derived.vector_email_migration_metadata_eng1973 
    (migration_step, records_affected, notes)
VALUES 
    ('rollback_executed', 
     (SELECT COUNT(*) FROM derived.vector_email),
     'ENG-1973 migration rolled back. Restored original vector_email table and temp_vector_emails view from backup.');

-- Display final migration history including rollback
SELECT * FROM derived.vector_email_migration_metadata_eng1973
ORDER BY execution_timestamp;

-- ============================================================================
-- ROLLBACK STEP 7: Cleanup (Optional - only after verifying rollback success)
-- ============================================================================
-- Wait 30 days before dropping backup to ensure no issues

-- Drop backup table (ONLY after 30-day verification period)
-- DROP TABLE IF EXISTS derived.vector_email_backup_eng1973 CASCADE;

-- Drop migration metadata (ONLY after backup dropped)
-- DROP TABLE IF EXISTS derived.vector_email_migration_metadata_eng1973 CASCADE;

-- ============================================================================
-- ROLLBACK VERIFICATION: Final checks
-- ============================================================================

-- Verify original structure is restored
SELECT 
    column_name,
    data_type,
    character_maximum_length,
    is_nullable
FROM information_schema.columns
WHERE table_schema = 'derived'
  AND table_name = 'vector_email'
ORDER BY ordinal_position;

-- Verify data integrity
SELECT 
    'Final Rollback Verification' as summary,
    (SELECT COUNT(*) FROM derived.vector_email) as current_record_count,
    (SELECT COUNT(*) FROM derived.vector_email_backup_eng1973) as backup_record_count,
    (SELECT COUNT(*) FROM derived.temp_vector_emails) as view_record_count,
    CASE 
        WHEN (SELECT COUNT(*) FROM derived.vector_email) = (SELECT COUNT(*) FROM derived.vector_email_backup_eng1973)
             AND (SELECT COUNT(*) FROM derived.temp_vector_emails) = (SELECT COUNT(*) FROM derived.vector_email)
        THEN '✓ ROLLBACK SUCCESSFUL - All data restored'
        ELSE '✗ WARNING - Verify record counts manually'
    END as status;

