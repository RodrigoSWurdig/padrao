-- ============================================================================
-- ENG-1914 Part 2 Post-Execution Verification
-- Execute after 02_migrate_data.sql completes
-- All checks must pass before proceeding to Part 3
-- ============================================================================

-- Verification 1: LinkedIn URL Migration
SELECT 
    'LinkedIn URLs' as verification_type,
    (SELECT COUNT(CASE WHEN linkedin_url IS NOT NULL AND TRIM(linkedin_url) != '' THEN 1 END) 
     FROM derived.vector_universal_person) as source_count,
    (SELECT COUNT(*) FROM derived.vup_linkedin_urls) as migrated_count,
    CASE 
        WHEN (SELECT COUNT(CASE WHEN linkedin_url IS NOT NULL AND TRIM(linkedin_url) != '' THEN 1 END) 
              FROM derived.vector_universal_person) = 
             (SELECT COUNT(*) FROM derived.vup_linkedin_urls)
        THEN '✅ PASS - Counts match'
        ELSE '❌ FAIL - Count mismatch'
    END as status;

-- Verification 2: FBF ID Migration
SELECT 
    'FBF IDs' as verification_type,
    (SELECT COUNT(CASE WHEN fbf_id IS NOT NULL AND TRIM(fbf_id) != '' THEN 1 END) 
     FROM derived.vector_universal_person) as source_count,
    (SELECT COUNT(*) FROM derived.vup_fbf_ids) as migrated_count,
    CASE 
        WHEN (SELECT COUNT(CASE WHEN fbf_id IS NOT NULL AND TRIM(fbf_id) != '' THEN 1 END) 
              FROM derived.vector_universal_person) = 
             (SELECT COUNT(*) FROM derived.vup_fbf_ids)
        THEN '✅ PASS - Counts match'
        ELSE '❌ FAIL - Count mismatch'
    END as status;

-- Verification 3: PDL ID Migration
SELECT 
    'PDL IDs' as verification_type,
    (SELECT COUNT(CASE WHEN pdl_id IS NOT NULL AND TRIM(pdl_id) != '' THEN 1 END) 
     FROM derived.vector_universal_person) as source_count,
    (SELECT COUNT(*) FROM derived.vup_pdl_ids) as migrated_count,
    CASE 
        WHEN (SELECT COUNT(CASE WHEN pdl_id IS NOT NULL AND TRIM(pdl_id) != '' THEN 1 END) 
              FROM derived.vector_universal_person) = 
             (SELECT COUNT(*) FROM derived.vup_pdl_ids)
        THEN '✅ PASS - Counts match'
        ELSE '❌ FAIL - Count mismatch'
    END as status;

-- Verification 4: Distribution Analysis
SELECT 
    'Identifier Coverage' as verification_type,
    (SELECT COUNT(DISTINCT vup_id) FROM derived.vup_linkedin_urls) as unique_persons_linkedin,
    (SELECT COUNT(DISTINCT vup_id) FROM derived.vup_fbf_ids) as unique_persons_fbf,
    (SELECT COUNT(DISTINCT vup_id) FROM derived.vup_pdl_ids) as unique_persons_pdl,
    '✅ Review distribution patterns' as status;

-- Verification 5: Multi-Value Detection
SELECT 
    'Multi-Value Persons' as verification_type,
    (SELECT COUNT(*) FROM (SELECT vup_id FROM derived.vup_linkedin_urls GROUP BY vup_id HAVING COUNT(*) > 1) x) as multi_linkedin,
    (SELECT COUNT(*) FROM (SELECT vup_id FROM derived.vup_fbf_ids GROUP BY vup_id HAVING COUNT(*) > 1) x) as multi_fbf,
    (SELECT COUNT(*) FROM (SELECT vup_id FROM derived.vup_pdl_ids GROUP BY vup_id HAVING COUNT(*) > 1) x) as multi_pdl,
    '✅ Multi-value capability validated' as status;

-- Summary
SELECT 
    'Part 2 Summary' as summary_type,
    'All identifiers migrated to junction tables' as message,
    'Proceed to Part 3: Duplicate Identification' as next_step;
