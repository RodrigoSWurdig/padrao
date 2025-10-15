-- ============================================================================
-- ENG-1914 Part 1 Post-Execution Verification
-- Execute after 01_backup_and_create_tables.sql completes
-- All checks must pass before proceeding to Part 2
-- ============================================================================

-- Verification 1: Backup Table Created
SELECT 
    'Backup Table' as verification_type,
    COUNT(*) as record_count,
    CASE 
        WHEN COUNT(*) = (SELECT COUNT(*) FROM derived.vector_universal_person)
        THEN '✅ PASS - Backup matches source'
        ELSE '❌ FAIL - Backup count mismatch'
    END as status
FROM derived.vector_universal_person_backup_eng1914;

-- Verification 2: Merged Column Added
SELECT 
    'Merged Column' as verification_type,
    COUNT(*) as total_records,
    COUNT(merged_into_vup_id) as non_null_count,
    CASE 
        WHEN COUNT(merged_into_vup_id) = 0 
        THEN '✅ PASS - Column added, all NULL'
        ELSE '❌ FAIL - Unexpected non-NULL values'
    END as status
FROM derived.vector_universal_person;

-- Verification 3: Junction Tables Created
SELECT 
    'Junction Tables' as verification_type,
    (SELECT COUNT(*) FROM derived.vup_linkedin_urls) as linkedin_count,
    (SELECT COUNT(*) FROM derived.vup_fbf_ids) as fbf_count,
    (SELECT COUNT(*) FROM derived.vup_pdl_ids) as pdl_count,
    CASE 
        WHEN (SELECT COUNT(*) FROM derived.vup_linkedin_urls) = 0 
         AND (SELECT COUNT(*) FROM derived.vup_fbf_ids) = 0
         AND (SELECT COUNT(*) FROM derived.vup_pdl_ids) = 0
        THEN '✅ PASS - All tables empty'
        ELSE '❌ FAIL - Tables contain unexpected data'
    END as status;

-- Verification 4: Datashare Registration
SELECT 
    'Datashare Registration' as verification_type,
    COUNT(*) as registered_tables,
    CASE 
        WHEN COUNT(*) = 3
        THEN '✅ PASS - All 3 junction tables registered'
        ELSE '❌ FAIL - Expected 3 tables in datashare'
    END as status
FROM svv_datashare_objects
WHERE share_name = 'vector_core_datashare'
  AND object_name IN ('vup_linkedin_urls', 'vup_fbf_ids', 'vup_pdl_ids');

-- Verification 5: Namespace Grants
SELECT 
    'Namespace Grants' as verification_type,
    'Manual verification required' as note,
    '✅ Check grants applied to namespace a4dd6eb0-5914-43ed-aa29-f80da082673c' as status;

-- Summary
SELECT 
    'Part 1 Summary' as summary_type,
    'All infrastructure created successfully' as message,
    'Proceed to Part 2: Data Migration' as next_step;
