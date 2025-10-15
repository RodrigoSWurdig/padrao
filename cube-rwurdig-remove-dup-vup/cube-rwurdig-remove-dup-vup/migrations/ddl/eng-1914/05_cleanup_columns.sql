-- ============================================================================
-- ENG-1914 Part 5: Remove Legacy Identifier Columns from Person Table
-- Purpose: Complete migration to junction table architecture
-- Operations: DESTRUCTIVE - Removes linkedin_url, fbf_id, pdl_id columns
-- CRITICAL PREREQUISITES:
--   1. Updated cube models deployed to production
--   2. Cube deployment validated with test queries
--   4. All downstream consumers migrated to new access patterns
--   5. Part 4 merge completed successfully and verified
-- WARNING: Do not execute until all prerequisites verified
-- ============================================================================

-- Pre-execution verification of prerequisites
SELECT 
    'Pre-Cleanup Prerequisites Check' as checkpoint_type,
    (SELECT COUNT(*) FROM derived.vector_universal_person 
     WHERE merged_into_vup_id IS NULL) as active_person_count,
    (SELECT COUNT(*) FROM derived.vector_universal_person 
     WHERE merged_into_vup_id IS NOT NULL) as merged_person_count,
    (SELECT COUNT(*) FROM derived.vup_linkedin_urls) as linkedin_junction_populated,
    (SELECT COUNT(*) FROM derived.vup_fbf_ids) as fbf_junction_populated,
    (SELECT COUNT(*) FROM derived.vup_pdl_ids) as pdl_junction_populated,
    CASE 
        WHEN (SELECT COUNT(*) FROM derived.vup_linkedin_urls) > 0
         AND (SELECT COUNT(*) FROM derived.vup_fbf_ids) > 0
         AND (SELECT COUNT(*) FROM derived.vup_pdl_ids) > 0
        THEN 'Junction tables populated - Ready for cleanup'
        ELSE 'ERROR - Junction tables empty, verify Parts 1-4 completion'
    END as data_readiness,
    'MANUAL VERIFICATION REQUIRED: Confirm cube models deployed and validated' as cube_deployment_reminder,
    'MANUAL VERIFICATION REQUIRED: Confirm 24-48 hour stabilization completed' as stabilization_reminder;

-- Display current schema for reference before modification
SELECT 
    'Current Person Table Schema' as schema_type,
    column_name,
    data_type,
    character_maximum_length,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_schema = 'derived'
  AND table_name = 'vector_universal_person'
ORDER BY ordinal_position;

-- Step 1: Create new person table with corrected schema (identifiers removed)
DROP TABLE IF EXISTS derived.vector_universal_person_new;

CREATE TABLE derived.vector_universal_person_new (
    vup_id VARCHAR(32) NOT NULL ENCODE raw,
    -- REMOVED COLUMNS: linkedin_url, fbf_id, pdl_id
    -- These identifiers now accessed via junction tables
    first_name VARCHAR(1024) ENCODE lzo,
    last_name VARCHAR(1024) ENCODE lzo,
    street_address VARCHAR(1024) ENCODE lzo,
    address_line_2 VARCHAR(1024) ENCODE lzo,
    locality VARCHAR(1024) ENCODE lzo,
    region VARCHAR(1024) ENCODE lzo,
    zip VARCHAR(1024) ENCODE lzo,
    country VARCHAR(1024) ENCODE lzo,
    gender VARCHAR(1024) ENCODE lzo,
    merged_into_vup_id VARCHAR(32) ENCODE lzo,
    data_source VARCHAR(1024) ENCODE lzo,
    dataset_version VARCHAR(1024) ENCODE lzo,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT ('now'::character varying)::timestamp with time zone ENCODE az64,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT ('now'::character varying)::timestamp with time zone ENCODE az64,
    PRIMARY KEY (vup_id)
) 
DISTSTYLE KEY 
DISTKEY(vup_id) 
SORTKEY(vup_id);

COMMENT ON TABLE derived.vector_universal_person_new IS 
'ENG-1914 updated person table with identifier columns removed. LinkedIn URLs, FBF IDs, and PDL IDs now accessed via junction tables (vup_linkedin_urls, vup_fbf_ids, vup_pdl_ids).';

COMMENT ON COLUMN derived.vector_universal_person_new.merged_into_vup_id IS 
'References the winning vup_id if this person was merged during ENG-1914 deduplication. NULL indicates active non-merged person. Use WHERE merged_into_vup_id IS NULL to query only active persons.';

-- Step 2: Migrate all data to new table (excluding removed identifier columns)
INSERT INTO derived.vector_universal_person_new (
    vup_id,
    first_name,
    last_name,
    street_address,
    address_line_2,
    locality,
    region,
    zip,
    country,
    gender,
    merged_into_vup_id,
    data_source,
    dataset_version,
    created_at,
    updated_at
)
SELECT 
    vup_id,
    first_name,
    last_name,
    street_address,
    address_line_2,
    locality,
    region,
    zip,
    country,
    gender,
    merged_into_vup_id,
    data_source,
    dataset_version,
    created_at,
    updated_at
FROM derived.vector_universal_person;

-- Comprehensive migration verification
SELECT 
    'Schema Migration Verification' as verification_type,
    (SELECT COUNT(*) FROM derived.vector_universal_person) as original_total_count,
    (SELECT COUNT(*) FROM derived.vector_universal_person_new) as new_table_total_count,
    (SELECT COUNT(*) FROM derived.vector_universal_person 
     WHERE merged_into_vup_id IS NULL) as original_active_count,
    (SELECT COUNT(*) FROM derived.vector_universal_person_new 
     WHERE merged_into_vup_id IS NULL) as new_table_active_count,
    (SELECT COUNT(*) FROM derived.vector_universal_person 
     WHERE merged_into_vup_id IS NOT NULL) as original_merged_count,
    (SELECT COUNT(*) FROM derived.vector_universal_person_new 
     WHERE merged_into_vup_id IS NOT NULL) as new_table_merged_count,
    CASE 
        WHEN (SELECT COUNT(*) FROM derived.vector_universal_person) = 
             (SELECT COUNT(*) FROM derived.vector_universal_person_new)
         AND (SELECT COUNT(DISTINCT vup_id) FROM derived.vector_universal_person) = 
             (SELECT COUNT(DISTINCT vup_id) FROM derived.vector_universal_person_new)
        THEN 'MIGRATION SUCCESSFUL - All records transferred'
        ELSE 'MIGRATION FAILED - Count mismatch detected - DO NOT PROCEED'
    END as migration_status;

-- Sample data verification
SELECT 
    'Sample Data Integrity Check' as check_type,
    orig.vup_id,
    orig.first_name = new.first_name as first_name_matches,
    orig.last_name = new.last_name as last_name_matches,
    orig.merged_into_vup_id = new.merged_into_vup_id as merged_field_matches,
    'All fields match between old and new tables' as expected_result
FROM derived.vector_universal_person orig
INNER JOIN derived.vector_universal_person_new new ON orig.vup_id = new.vup_id
LIMIT 10;

-- Step 3: Execute atomic table swap (requires brief exclusive lock)
BEGIN TRANSACTION;

-- Rename original table to preserve for rollback capability
ALTER TABLE derived.vector_universal_person 
RENAME TO vector_universal_person_old_eng1914;

-- Rename new table to production name
ALTER TABLE derived.vector_universal_person_new 
RENAME TO vector_universal_person;

-- Verify swap completed successfully
SELECT 
    'Table Swap Verification' as verification_type,
    (SELECT COUNT(*) FROM derived.vector_universal_person) as current_production_count,
    (SELECT COUNT(*) FROM derived.vector_universal_person 
     WHERE merged_into_vup_id IS NULL) as current_production_active_count,
    (SELECT COUNT(*) FROM derived.vector_universal_person_old_eng1914) as old_table_preserved_count,
    'Old table preserved as vector_universal_person_old_eng1914 for rollback' as preservation_note,
    CASE 
        WHEN (SELECT COUNT(*) FROM derived.vector_universal_person) > 0
        THEN 'TABLE SWAP SUCCESSFUL'
        ELSE 'TABLE SWAP FAILED - ROLLBACK IMMEDIATELY'
    END as swap_status;

COMMIT;

-- Step 4: Update permissions and datashare registration
GRANT SELECT ON derived.vector_universal_person TO GROUP analytics_users;
GRANT SELECT ON derived.vector_universal_person TO GROUP engineering_users;

ALTER DATASHARE vector_core_datashare ADD TABLE derived.vector_universal_person;

-- Verify datashare registration
SELECT 
    'Datashare Registration Verification' as verification_type,
    share_name,
    object_type,
    object_name,
    'Updated person table registered in datashare for cube access' as status
FROM svv_datashare_objects
WHERE share_name = 'vector_core_datashare'
  AND object_name = 'vector_universal_person';

-- Step 5: Verify final schema structure
SELECT 
    'Final Schema Verification' as verification_type,
    column_name,
    data_type,
    character_maximum_length,
    is_nullable
FROM information_schema.columns
WHERE table_schema = 'derived'
  AND table_name = 'vector_universal_person'
ORDER BY ordinal_position;

-- Verify identifier columns successfully removed
SELECT 
    'Identifier Column Removal Verification' as verification_type,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_schema = 'derived' 
              AND table_name = 'vector_universal_person' 
              AND column_name IN ('linkedin_url', 'fbf_id', 'pdl_id')
        )
        THEN 'FAILED - Identifier columns still present in schema'
        ELSE 'SUCCESS - Identifier columns removed, access via junction tables'
    END as removal_status;

-- Step 6: Storage impact analysis
SELECT 
    'Storage Impact Analysis' as analysis_type,
    pg_size_pretty(pg_total_relation_size('derived.vector_universal_person'::regclass)) as new_person_table_size,
    pg_size_pretty(pg_total_relation_size('derived.vector_universal_person_old_eng1914'::regclass)) as old_person_table_size,
    pg_size_pretty(
        pg_total_relation_size('derived.vector_universal_person_old_eng1914'::regclass) - 
        pg_total_relation_size('derived.vector_universal_person'::regclass)
    ) as storage_space_saved,
    pg_size_pretty(
        pg_total_relation_size('derived.vup_linkedin_urls'::regclass) +
        pg_total_relation_size('derived.vup_fbf_ids'::regclass) +
        pg_total_relation_size('derived.vup_pdl_ids'::regclass)
    ) as junction_tables_total_size;

-- Step 7: Validate cube access patterns still functional
-- This queries the new structure to ensure junction table joins work correctly
SELECT 
    'Cube Access Pattern Validation' as validation_type,
    COUNT(DISTINCT vup.vup_id) as persons_in_new_table,
    COUNT(DISTINCT lu.vup_id) as persons_accessible_via_linkedin_junction,
    COUNT(DISTINCT fbf.vup_id) as persons_accessible_via_fbf_junction,
    COUNT(DISTINCT pdl.vup_id) as persons_accessible_via_pdl_junction,
    'Junction table joins functional for cube access' as access_pattern_status
FROM derived.vector_universal_person vup
LEFT JOIN derived.vup_linkedin_urls lu ON vup.vup_id = lu.vup_id
LEFT JOIN derived.vup_fbf_ids fbf ON vup.vup_id = fbf.vup_id
LEFT JOIN derived.vup_pdl_ids pdl ON vup.vup_id = pdl.vup_id
WHERE vup.merged_into_vup_id IS NULL;

-- Step 8: Generate comprehensive completion report
SELECT 
    'ENG-1914 Schema Cleanup Complete' as summary_type,
    (SELECT COUNT(*) FROM derived.vector_universal_person 
     WHERE merged_into_vup_id IS NULL) as final_active_person_count,
    (SELECT COUNT(*) FROM derived.vector_universal_person 
     WHERE merged_into_vup_id IS NOT NULL) as final_merged_person_count,
    (SELECT COUNT(*) FROM derived.vup_linkedin_urls) as linkedin_urls_in_junction,
    (SELECT COUNT(*) FROM derived.vup_fbf_ids) as fbf_ids_in_junction,
    (SELECT COUNT(*) FROM derived.vup_pdl_ids) as pdl_ids_in_junction,
    'Legacy identifier columns removed from person table' as schema_change,
    'Identifiers now accessed via junction tables with one-to-many support' as new_architecture,
    'Cube models provide seamless access to identifier relationships' as access_layer,
    'Old table preserved for 30 days as vector_universal_person_old_eng1914' as rollback_capability,
    GETDATE() as migration_completed_at;

-- Cleanup recommendation for future execution (after 30 days of stable production)
SELECT 
    'Future Cleanup Recommendation' as recommendation_type,
    'After 30 days of stable production operation, execute:' as timing,
    'DROP TABLE derived.vector_universal_person_old_eng1914;' as cleanup_old_table,
    'DROP TABLE derived.vector_universal_person_backup_eng1914;' as cleanup_backup_table,
    'This will free significant storage space' as benefit;

