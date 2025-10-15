-- ============================================================================
-- ENG-1914 ROLLBACK SCRIPT - Use only if issues occur
-- WARNING: Choose ONE rollback option based on your situation
-- ============================================================================

-- ============================================================================
-- ROLLBACK OPTION 1: COMPLETE RESTORE FROM BACKUP (Parts 1-5 Rollback)
-- ============================================================================

-- This is the most comprehensive rollback - restores everything to pre-migration state
-- Use this if you need to completely undo all migration changes
-- REQUIRES: vector_universal_person_backup_eng1914 table must exist

BEGIN TRANSACTION;
    -- Drop current table
    DROP TABLE IF EXISTS derived.vector_universal_person;
    
    -- Recreate table structure from backup to preserve defaults/identity
    CREATE TABLE derived.vector_universal_person
    (LIKE derived.vector_universal_person_backup_eng1914 INCLUDING DEFAULTS);

    -- Restore data
    INSERT INTO derived.vector_universal_person
    SELECT * FROM derived.vector_universal_person_backup_eng1914;
    
    -- Restore distribution and sort keys
    ALTER TABLE derived.vector_universal_person 
    ALTER DISTSTYLE KEY;
    
    ALTER TABLE derived.vector_universal_person 
    ALTER DISTKEY vup_id;
    
    ALTER TABLE derived.vector_universal_person 
    ALTER SORTKEY (vup_id);
    
    -- Drop junction tables
    DROP TABLE IF EXISTS derived.vup_linkedin_urls;
    DROP TABLE IF EXISTS derived.vup_fbf_ids;
    DROP TABLE IF EXISTS derived.vup_pdl_ids;
COMMIT;

-- Restore datashare registration (must run outside explicit transaction)
ALTER DATASHARE vector_core_datashare ADD TABLE derived.vector_universal_person;

-- Restore permissions
GRANT SELECT ON derived.vector_universal_person TO NAMESPACE 'a4dd6eb0-5914-43ed-aa29-f80da082673c';

-- Verify Option 1 rollback
WITH verification AS (
    SELECT 
        (SELECT COUNT(*) FROM derived.vector_universal_person) AS total_persons,
        (SELECT COUNT(*) FROM svv_datashare_objects WHERE share_name = 'vector_core_datashare' AND object_name = 'vector_universal_person') AS datashare_registration_count,
        (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'derived' AND table_name IN ('vup_linkedin_urls', 'vup_fbf_ids', 'vup_pdl_ids')) AS junction_tables_remaining
)
SELECT 
    'Option 1 Rollback Verification' as check_type,
    total_persons,
    datashare_registration_count,
    junction_tables_remaining,
    CASE 
        WHEN junction_tables_remaining = 0 AND datashare_registration_count = 1
        THEN 'ROLLBACK SUCCESSFUL - BASE TABLE RESTORED AND JUNCTION TABLES REMOVED'
        WHEN junction_tables_remaining = 0 AND datashare_registration_count = 0
        THEN 'ACTION REQUIRED - RE-ADD VECTOR_UNIVERSAL_PERSON TO DATASHARE'
        ELSE 'ROLLBACK INCOMPLETE - REVIEW COUNTS ABOVE'
    END as verification_status
FROM verification;

-- Verify restored table structure
SELECT 
    'Table Structure Verification' as check_type,
    column_name,
    data_type,
    character_maximum_length
FROM information_schema.columns
WHERE table_schema = 'derived'
  AND table_name = 'vector_universal_person'
ORDER BY ordinal_position;

-- ============================================================================
-- ROLLBACK OPTION 2: REMOVE SOFT DELETES (Part 4 Rollback)
-- ============================================================================

-- This undoes the merge operation by clearing merged_into_vup_id
-- WARNING: This does not restore child table relationships
-- Child tables will still point to winner records after this rollback
UPDATE derived.vector_universal_person
SET merged_into_vup_id = NULL,
    updated_at = GETDATE()
WHERE merged_into_vup_id IS NOT NULL;

-- Verify soft delete removal
SELECT 
    'Soft Delete Removal Verification' as check_type,
    COUNT(*) as total_records,
    COUNT(CASE WHEN merged_into_vup_id IS NULL THEN 1 END) as active_records,
    COUNT(CASE WHEN merged_into_vup_id IS NOT NULL THEN 1 END) as merged_records_remaining,
    CASE 
        WHEN COUNT(CASE WHEN merged_into_vup_id IS NOT NULL THEN 1 END) = 0
        THEN 'ALL SOFT DELETES REMOVED'
        ELSE 'SOFT DELETES STILL PRESENT'
    END as verification_status
FROM derived.vector_universal_person;

-- ============================================================================
-- ROLLBACK OPTION 3: DROP JUNCTION TABLES (Complete Rollback)
-- ============================================================================

-- WARNING: This loses the one-to-many capability
-- Only use if you want to completely reverse the schema changes
-- Uncomment to execute:

-- DROP TABLE IF EXISTS derived.vup_linkedin_urls;
-- DROP TABLE IF EXISTS derived.vup_fbf_ids;
-- DROP TABLE IF EXISTS derived.vup_pdl_ids;

-- ============================================================================
-- ROLLBACK OPTION 4: REDSHIFT SNAPSHOT RESTORE (NUCLEAR OPTION)
-- ============================================================================

-- If a Redshift snapshot was created before migration, use AWS console to:
-- 1. Identify the snapshot created before migration execution
-- 2. Restore from snapshot to a new cluster or restore specific tables
-- 3. Update connection strings in applications to point to restored data
-- This is the most comprehensive rollback but also the most disruptive

-- ============================================================================
-- POST-ROLLBACK VERIFICATION
-- ============================================================================

-- Verify database state after rollback
SELECT 
    'Post-Rollback State' as verification_type,
    (SELECT COUNT(*) FROM derived.vector_universal_person) as total_persons,
    (SELECT COUNT(*) FROM derived.vector_universal_person WHERE merged_into_vup_id IS NULL) as active_persons,
    (SELECT COUNT(*) FROM derived.vector_universal_person WHERE merged_into_vup_id IS NOT NULL) as merged_persons,
    (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'derived' AND table_name IN ('vup_linkedin_urls', 'vup_fbf_ids', 'vup_pdl_ids')) as junction_tables_remaining,
    (SELECT COUNT(*) FROM svv_datashare_objects WHERE share_name = 'vector_core_datashare' AND object_name = 'vector_universal_person') as datashare_registration_count;

