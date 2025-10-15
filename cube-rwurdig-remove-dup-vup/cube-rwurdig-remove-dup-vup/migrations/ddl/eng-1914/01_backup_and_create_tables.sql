-- ============================================================================
-- ENG-1914 Part 1: Backup and Create Infrastructure 
-- SAFE TO RUN: This script is purely additive with no destructive operations
-- ============================================================================

-- ============================================================================
-- STEP 1: CREATE BACKUP TABLE FOR ROLLBACK CAPABILITY
-- ============================================================================

-- Create complete backup of vector_universal_person for rollback capability
-- This backup preserves the exact state before any modifications
CREATE TABLE derived.vector_universal_person_backup_eng1914 
(LIKE derived.vector_universal_person INCLUDING DEFAULTS);

-- Populate backup table with complete current state
INSERT INTO derived.vector_universal_person_backup_eng1914
SELECT * FROM derived.vector_universal_person;

-- Verify backup was created successfully
SELECT 
    'Backup Verification' as check_type,
    COUNT(*) as backup_record_count
FROM derived.vector_universal_person_backup_eng1914;

-- ============================================================================
-- STEP 2: ADD SOFT-DELETE TRACKING COLUMN
-- ============================================================================

-- Add merged_into_vup_id column to support soft-delete deduplication pattern
-- This column will point to the winning record when duplicates are merged
ALTER TABLE derived.vector_universal_person 
ADD COLUMN merged_into_vup_id VARCHAR(32) ENCODE lzo DEFAULT NULL;

-- Verify column addition succeeded
SELECT 
    'Column Addition Verification' as check_type,
    COUNT(*) as total_records,
    COUNT(merged_into_vup_id) as non_null_merged_records,
    CASE 
        WHEN COUNT(merged_into_vup_id) = 0 
        THEN 'COLUMN ADDED SUCCESSFULLY - ALL NULL AS EXPECTED'
        ELSE 'WARNING - UNEXPECTED NON-NULL VALUES PRESENT'
    END as verification_status
FROM derived.vector_universal_person;

-- ============================================================================
-- STEP 3: CREATE JUNCTION TABLES FOR IDENTIFIERS
-- ============================================================================

-- Create new table for LinkedIn URLs (one-to-many with vup_id)
CREATE TABLE derived.vup_linkedin_urls (
  vup_id VARCHAR(32) NOT NULL ENCODE raw,
  linkedin_url VARCHAR(1024) NOT NULL ENCODE lzo,
  created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT GETDATE() ENCODE az64,
  PRIMARY KEY (vup_id, linkedin_url)
) DISTSTYLE KEY 
  DISTKEY(vup_id) 
  SORTKEY(vup_id, linkedin_url);

-- Create new table for FBF IDs
CREATE TABLE derived.vup_fbf_ids (
  vup_id VARCHAR(32) NOT NULL ENCODE raw,
  fbf_id VARCHAR(256) NOT NULL ENCODE lzo,
  created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT GETDATE() ENCODE az64,
  PRIMARY KEY (vup_id, fbf_id)
) DISTSTYLE KEY 
  DISTKEY(vup_id) 
  SORTKEY(vup_id, fbf_id);

-- Create new table for PDL IDs
CREATE TABLE derived.vup_pdl_ids (
  vup_id VARCHAR(32) NOT NULL ENCODE raw,
  pdl_id VARCHAR(256) NOT NULL ENCODE lzo,
  created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT GETDATE() ENCODE az64,
  PRIMARY KEY (vup_id, pdl_id)
) DISTSTYLE KEY 
  DISTKEY(vup_id) 
  SORTKEY(vup_id, pdl_id);

-- ============================================================================
-- STEP 4: VERIFY JUNCTION TABLES CREATED
-- ============================================================================

-- Verify all junction tables exist and are empty
SELECT 
    'Junction Tables Created' as check_type,
    (SELECT COUNT(*) FROM derived.vup_linkedin_urls) as linkedin_count,
    (SELECT COUNT(*) FROM derived.vup_fbf_ids) as fbf_count,
    (SELECT COUNT(*) FROM derived.vup_pdl_ids) as pdl_count;

-- ============================================================================
-- STEP 5: REGISTER JUNCTION TABLES WITH DATASHARE
-- ============================================================================

-- Explicitly add new junction tables to datashare for consumer access
-- While INCLUDENEW should handle this automatically, explicit addition ensures deterministic behavior
ALTER DATASHARE vector_core_datashare ADD TABLE derived.vup_linkedin_urls;
ALTER DATASHARE vector_core_datashare ADD TABLE derived.vup_fbf_ids;
ALTER DATASHARE vector_core_datashare ADD TABLE derived.vup_pdl_ids;

-- Verify junction tables are registered in datashare
SELECT 
    'Datashare Registration' as check_type,
    object_name,
    object_type,
    share_name
FROM svv_datashare_objects
WHERE share_name = 'vector_core_datashare'
  AND object_name IN ('vup_linkedin_urls', 'vup_fbf_ids', 'vup_pdl_ids')
ORDER BY object_name;

-- ============================================================================
-- STEP 6: GRANT PERMISSIONS ON NEW TABLES
-- ============================================================================

-- Grant SELECT permissions to segment evaluation namespace
-- This namespace ID comes from your DATASHARE_PRODUCER.sql configuration
GRANT SELECT ON derived.vup_linkedin_urls TO NAMESPACE 'a4dd6eb0-5914-43ed-aa29-f80da082673c';
GRANT SELECT ON derived.vup_fbf_ids TO NAMESPACE 'a4dd6eb0-5914-43ed-aa29-f80da082673c';
GRANT SELECT ON derived.vup_pdl_ids TO NAMESPACE 'a4dd6eb0-5914-43ed-aa29-f80da082673c';

-- ============================================================================
-- STEP 7: FINAL VERIFICATION
-- ============================================================================

-- Comprehensive verification of all infrastructure creation
SELECT 
    'Infrastructure Creation Summary' as summary_type,
    (SELECT COUNT(*) FROM derived.vector_universal_person_backup_eng1914) as backup_record_count,
    (SELECT COUNT(*) FROM derived.vup_linkedin_urls) as linkedin_table_count,
    (SELECT COUNT(*) FROM derived.vup_fbf_ids) as fbf_table_count,
    (SELECT COUNT(*) FROM derived.vup_pdl_ids) as pdl_table_count,
    (SELECT COUNT(*) FROM svv_datashare_objects WHERE share_name = 'vector_core_datashare' AND object_name IN ('vup_linkedin_urls', 'vup_fbf_ids', 'vup_pdl_ids')) as tables_in_datashare,
    CASE 
        WHEN (SELECT COUNT(*) FROM derived.vup_linkedin_urls) = 0 
         AND (SELECT COUNT(*) FROM derived.vup_fbf_ids) = 0
         AND (SELECT COUNT(*) FROM derived.vup_pdl_ids) = 0
         AND (SELECT COUNT(*) FROM svv_datashare_objects WHERE share_name = 'vector_core_datashare' AND object_name IN ('vup_linkedin_urls', 'vup_fbf_ids', 'vup_pdl_ids')) = 3
        THEN 'ALL INFRASTRUCTURE CREATED SUCCESSFULLY'
        ELSE 'REVIEW REQUIRED - UNEXPECTED STATE'
    END as verification_status;

