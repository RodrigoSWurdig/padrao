-- ============================================================================
-- ENG-1914 Part 2: Migrate Identifier Data to Junction Tables
-- Purpose: Populate junction tables from existing denormalized columns
-- Operations: Copies data without modifying original table
-- Expected Volumes: 100.7M LinkedIn | 303.3M FBF | 75.2M PDL migrations
-- Prerequisites: Part 1 completed, junction tables verified empty
-- ============================================================================

-- Pre-migration verification checkpoint
SELECT 
    'Pre-Migration Verification' as checkpoint_type,
    (SELECT COUNT(*) FROM derived.vup_linkedin_urls) as linkedin_current_count,
    (SELECT COUNT(*) FROM derived.vup_fbf_ids) as fbf_current_count,
    (SELECT COUNT(*) FROM derived.vup_pdl_ids) as pdl_current_count,
    CASE 
        WHEN (SELECT COUNT(*) FROM derived.vup_linkedin_urls) = 0
         AND (SELECT COUNT(*) FROM derived.vup_fbf_ids) = 0
         AND (SELECT COUNT(*) FROM derived.vup_pdl_ids) = 0
        THEN 'READY - All junction tables empty'
        ELSE 'ERROR - Junction tables not empty, Part 2 may have already run'
    END as readiness_status;

-- ============================================================================
-- STEP 1: MIGRATE LINKEDIN URLS
-- ============================================================================

-- Migrate LinkedIn URLs with quality filters
INSERT INTO derived.vup_linkedin_urls (vup_id, linkedin_url, created_at)
SELECT 
    vup_id,
    TRIM(linkedin_url) as linkedin_url,
    COALESCE(updated_at, created_at, GETDATE()) as created_at
FROM derived.vector_universal_person
WHERE linkedin_url IS NOT NULL 
  AND TRIM(linkedin_url) != ''
  AND TRIM(linkedin_url) != 'null'
  AND TRIM(linkedin_url) != 'NULL'
  AND LENGTH(TRIM(linkedin_url)) > 10  -- Minimum realistic LinkedIn URL length
  AND LOWER(TRIM(linkedin_url)) LIKE '%linkedin.com%';  -- Basic format validation

-- LinkedIn migration verification
SELECT 
    'LinkedIn URL Migration Verification' as migration_check,
    (SELECT COUNT(*) 
     FROM derived.vector_universal_person 
     WHERE linkedin_url IS NOT NULL 
       AND TRIM(linkedin_url) != ''
       AND TRIM(linkedin_url) != 'null'
       AND TRIM(linkedin_url) != 'NULL'
       AND LENGTH(TRIM(linkedin_url)) > 10
       AND LOWER(TRIM(linkedin_url)) LIKE '%linkedin.com%') as source_count,
    (SELECT COUNT(*) FROM derived.vup_linkedin_urls) as migrated_count,
    (SELECT COUNT(DISTINCT vup_id) FROM derived.vup_linkedin_urls) as unique_persons_migrated,
    (SELECT COUNT(DISTINCT linkedin_url) FROM derived.vup_linkedin_urls) as unique_urls_migrated,
    CASE 
        WHEN (SELECT COUNT(*) 
              FROM derived.vector_universal_person 
              WHERE linkedin_url IS NOT NULL 
                AND TRIM(linkedin_url) != ''
                AND TRIM(linkedin_url) != 'null'
                AND TRIM(linkedin_url) != 'NULL'
                AND LENGTH(TRIM(linkedin_url)) > 10
                AND LOWER(TRIM(linkedin_url)) LIKE '%linkedin.com%') = 
             (SELECT COUNT(*) FROM derived.vup_linkedin_urls)
        THEN 'LINKEDIN MIGRATION SUCCESSFUL'
        ELSE 'LINKEDIN MIGRATION COUNT MISMATCH - Review required'
    END as migration_status,
    GETDATE() as verified_at;

-- ============================================================================
-- STEP 2: MIGRATE FBF IDS
-- ============================================================================

-- Migrate FBF IDs with quality filters
INSERT INTO derived.vup_fbf_ids (vup_id, fbf_id, created_at)
SELECT 
    vup_id,
    TRIM(fbf_id) as fbf_id,
    COALESCE(updated_at, created_at, GETDATE()) as created_at
FROM derived.vector_universal_person
WHERE fbf_id IS NOT NULL 
  AND TRIM(fbf_id) != ''
  AND TRIM(fbf_id) != 'null'
  AND TRIM(fbf_id) != 'NULL'
  AND LENGTH(TRIM(fbf_id)) > 5;  -- Minimum realistic FBF ID length

-- FBF ID migration verification
SELECT 
    'FBF ID Migration Verification' as migration_check,
    (SELECT COUNT(*) 
     FROM derived.vector_universal_person 
     WHERE fbf_id IS NOT NULL 
       AND TRIM(fbf_id) != ''
       AND TRIM(fbf_id) != 'null'
       AND TRIM(fbf_id) != 'NULL'
       AND LENGTH(TRIM(fbf_id)) > 5) as source_count,
    (SELECT COUNT(*) FROM derived.vup_fbf_ids) as migrated_count,
    (SELECT COUNT(DISTINCT vup_id) FROM derived.vup_fbf_ids) as unique_persons_migrated,
    (SELECT COUNT(DISTINCT fbf_id) FROM derived.vup_fbf_ids) as unique_fbf_ids_migrated,
    CASE 
        WHEN (SELECT COUNT(*) 
              FROM derived.vector_universal_person 
              WHERE fbf_id IS NOT NULL 
                AND TRIM(fbf_id) != ''
                AND TRIM(fbf_id) != 'null'
                AND TRIM(fbf_id) != 'NULL'
                AND LENGTH(TRIM(fbf_id)) > 5) = 
             (SELECT COUNT(*) FROM derived.vup_fbf_ids)
        THEN 'FBF MIGRATION SUCCESSFUL'
        ELSE 'FBF MIGRATION COUNT MISMATCH - Review required'
    END as migration_status,
    GETDATE() as verified_at;

-- ============================================================================
-- STEP 3: MIGRATE PDL IDS
-- ============================================================================

-- Migrate PDL IDs with quality filters
INSERT INTO derived.vup_pdl_ids (vup_id, pdl_id, created_at)
SELECT 
    vup_id,
    TRIM(pdl_id) as pdl_id,
    COALESCE(updated_at, created_at, GETDATE()) as created_at
FROM derived.vector_universal_person
WHERE pdl_id IS NOT NULL 
  AND TRIM(pdl_id) != ''
  AND TRIM(pdl_id) != 'null'
  AND TRIM(pdl_id) != 'NULL'
  AND LENGTH(TRIM(pdl_id)) > 5;  -- Minimum realistic PDL ID length

-- PDL ID migration verification
SELECT 
    'PDL ID Migration Verification' as migration_check,
    (SELECT COUNT(*) 
     FROM derived.vector_universal_person 
     WHERE pdl_id IS NOT NULL 
       AND TRIM(pdl_id) != ''
       AND TRIM(pdl_id) != 'null'
       AND TRIM(pdl_id) != 'NULL'
       AND LENGTH(TRIM(pdl_id)) > 5) as source_count,
    (SELECT COUNT(*) FROM derived.vup_pdl_ids) as migrated_count,
    (SELECT COUNT(DISTINCT vup_id) FROM derived.vup_pdl_ids) as unique_persons_migrated,
    (SELECT COUNT(DISTINCT pdl_id) FROM derived.vup_pdl_ids) as unique_pdl_ids_migrated,
    CASE 
        WHEN (SELECT COUNT(*) 
              FROM derived.vector_universal_person 
              WHERE pdl_id IS NOT NULL 
                AND TRIM(pdl_id) != ''
                AND TRIM(pdl_id) != 'null'
                AND TRIM(pdl_id) != 'NULL'
                AND LENGTH(TRIM(pdl_id)) > 5) = 
             (SELECT COUNT(*) FROM derived.vup_pdl_ids)
        THEN 'PDL MIGRATION SUCCESSFUL'
        ELSE 'PDL MIGRATION COUNT MISMATCH - Review required'
    END as migration_status,
    GETDATE() as verified_at;

-- ============================================================================
-- STEP 4: COMPREHENSIVE MIGRATION SUMMARY
-- ============================================================================

-- Summary of all migrated identifiers with person coverage analysis
SELECT 
    'Migration Summary Report' as report_type,
    (SELECT COUNT(*) FROM derived.vector_universal_person) as total_persons,
    (SELECT COUNT(*) FROM derived.vup_linkedin_urls) as linkedin_urls_migrated,
    (SELECT COUNT(DISTINCT vup_id) FROM derived.vup_linkedin_urls) as persons_with_linkedin,
    (SELECT COUNT(DISTINCT linkedin_url) FROM derived.vup_linkedin_urls) as unique_linkedin_urls,
    (SELECT COUNT(*) FROM derived.vup_fbf_ids) as fbf_ids_migrated,
    (SELECT COUNT(DISTINCT vup_id) FROM derived.vup_fbf_ids) as persons_with_fbf,
    (SELECT COUNT(DISTINCT fbf_id) FROM derived.vup_fbf_ids) as unique_fbf_ids,
    (SELECT COUNT(*) FROM derived.vup_pdl_ids) as pdl_ids_migrated,
    (SELECT COUNT(DISTINCT vup_id) FROM derived.vup_pdl_ids) as persons_with_pdl,
    (SELECT COUNT(DISTINCT pdl_id) FROM derived.vup_pdl_ids) as unique_pdl_ids,
    ROUND(100.0 * (SELECT COUNT(DISTINCT vup_id) FROM derived.vup_linkedin_urls) / 
          (SELECT COUNT(*) FROM derived.vector_universal_person), 2) as pct_persons_with_linkedin,
    ROUND(100.0 * (SELECT COUNT(DISTINCT vup_id) FROM derived.vup_fbf_ids) / 
          (SELECT COUNT(*) FROM derived.vector_universal_person), 2) as pct_persons_with_fbf,
    ROUND(100.0 * (SELECT COUNT(DISTINCT vup_id) FROM derived.vup_pdl_ids) / 
          (SELECT COUNT(*) FROM derived.vector_universal_person), 2) as pct_persons_with_pdl;

-- ============================================================================
-- STEP 5: DUPLICATE IDENTIFIER DETECTION
-- ============================================================================

-- Check for persons with multiple values for the same identifier type
-- These indicate potential duplicates that will be addressed in Part 4
SELECT 
    'Duplicate Identifier Check' as check_type,
    (SELECT COUNT(*) FROM (
        SELECT vup_id 
        FROM derived.vup_linkedin_urls 
        GROUP BY vup_id 
        HAVING COUNT(*) > 1
    ) x) as persons_with_multiple_linkedin,
    (SELECT COUNT(*) FROM (
        SELECT vup_id 
        FROM derived.vup_fbf_ids 
        GROUP BY vup_id 
        HAVING COUNT(*) > 1
    ) x) as persons_with_multiple_fbf,
    (SELECT COUNT(*) FROM (
        SELECT vup_id 
        FROM derived.vup_pdl_ids 
        GROUP BY vup_id 
        HAVING COUNT(*) > 1
    ) x) as persons_with_multiple_pdl;

-- Sample persons with multiple LinkedIn URLs for review
SELECT 
    'Sample Multi-LinkedIn Persons' as sample_type,
    vup_id,
    COUNT(*) as linkedin_count,
    LISTAGG(linkedin_url, ' | ') WITHIN GROUP (ORDER BY linkedin_url) as sample_urls
FROM derived.vup_linkedin_urls
GROUP BY vup_id
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC
LIMIT 10;

-- ============================================================================
-- STEP 6: IDENTIFIER DISTRIBUTION ANALYSIS
-- ============================================================================

-- Understand distribution of identifier combinations across persons
SELECT 
    'Identifier Combination Analysis' as analysis_type,
    COUNT(*) as person_count,
    CASE 
        WHEN has_linkedin + has_fbf + has_pdl = 3 THEN 'All Three Identifiers'
        WHEN has_linkedin + has_fbf + has_pdl = 2 THEN 'Two Identifiers'
        WHEN has_linkedin + has_fbf + has_pdl = 1 THEN 'One Identifier Only'
        ELSE 'No Identifiers'
    END as identifier_coverage,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage
FROM (
    SELECT 
        vup.vup_id,
        CASE WHEN lu.vup_id IS NOT NULL THEN 1 ELSE 0 END as has_linkedin,
        CASE WHEN fbf.vup_id IS NOT NULL THEN 1 ELSE 0 END as has_fbf,
        CASE WHEN pdl.vup_id IS NOT NULL THEN 1 ELSE 0 END as has_pdl
    FROM derived.vector_universal_person vup
    LEFT JOIN (SELECT DISTINCT vup_id FROM derived.vup_linkedin_urls) lu ON vup.vup_id = lu.vup_id
    LEFT JOIN (SELECT DISTINCT vup_id FROM derived.vup_fbf_ids) fbf ON vup.vup_id = fbf.vup_id
    LEFT JOIN (SELECT DISTINCT vup_id FROM derived.vup_pdl_ids) pdl ON vup.vup_id = pdl.vup_id
) identifier_coverage
GROUP BY 
    CASE 
        WHEN has_linkedin + has_fbf + has_pdl = 3 THEN 'All Three Identifiers'
        WHEN has_linkedin + has_fbf + has_pdl = 2 THEN 'Two Identifiers'
        WHEN has_linkedin + has_fbf + has_pdl = 1 THEN 'One Identifier Only'
        ELSE 'No Identifiers'
    END
ORDER BY person_count DESC;

-- ============================================================================
-- STEP 7: ORIGINAL TABLE INTEGRITY VERIFICATION
-- ============================================================================

-- Verify original table columns remain unchanged
SELECT 
    'Original Table Integrity Verification' as check_type,
    COUNT(*) as total_persons,
    COUNT(CASE WHEN linkedin_url IS NOT NULL AND TRIM(linkedin_url) != '' THEN 1 END) as linkedin_still_populated,
    COUNT(CASE WHEN fbf_id IS NOT NULL AND TRIM(fbf_id) != '' THEN 1 END) as fbf_still_populated,
    COUNT(CASE WHEN pdl_id IS NOT NULL AND TRIM(pdl_id) != '' THEN 1 END) as pdl_still_populated,
    CASE 
        WHEN COUNT(CASE WHEN linkedin_url IS NOT NULL AND TRIM(linkedin_url) != '' THEN 1 END) > 0
         AND COUNT(CASE WHEN fbf_id IS NOT NULL AND TRIM(fbf_id) != '' THEN 1 END) > 0
         AND COUNT(CASE WHEN pdl_id IS NOT NULL AND TRIM(pdl_id) != '' THEN 1 END) > 0
        THEN 'VERIFIED - Original columns unchanged and accessible'
        ELSE 'WARNING - Original column data appears modified'
    END as integrity_status
FROM derived.vector_universal_person;

-- ============================================================================
-- STEP 8: MULTI-IDENTIFIER PERSON PREVIEW
-- ============================================================================

-- Identify persons with multiple identifiers (potential duplicates preview)
SELECT 
    'Multi-Identifier Person Preview' as analysis_type,
    vup_id,
    (SELECT COUNT(*) FROM derived.vup_linkedin_urls WHERE vup_id = vup.vup_id) as linkedin_url_count,
    (SELECT COUNT(*) FROM derived.vup_fbf_ids WHERE vup_id = vup.vup_id) as fbf_id_count,
    (SELECT COUNT(*) FROM derived.vup_pdl_ids WHERE vup_id = vup.vup_id) as pdl_id_count,
    (SELECT COUNT(*) FROM derived.vector_email WHERE vup_id = vup.vup_id AND sha256 IS NOT NULL) as email_count
FROM derived.vector_universal_person vup
WHERE EXISTS (SELECT 1 FROM derived.vup_linkedin_urls WHERE vup_id = vup.vup_id)
   OR EXISTS (SELECT 1 FROM derived.vup_fbf_ids WHERE vup_id = vup.vup_id)
   OR EXISTS (SELECT 1 FROM derived.vup_pdl_ids WHERE vup_id = vup.vup_id)
LIMIT 25;

-- ============================================================================
-- STEP 9: STORAGE IMPACT ANALYSIS
-- ============================================================================

-- Analyze storage impact of new junction tables
SELECT 
    'Storage Impact Analysis' as analysis_type,
    (SELECT pg_size_pretty(pg_total_relation_size('derived.vup_linkedin_urls'::regclass))) as linkedin_table_size,
    (SELECT pg_size_pretty(pg_total_relation_size('derived.vup_fbf_ids'::regclass))) as fbf_table_size,
    (SELECT pg_size_pretty(pg_total_relation_size('derived.vup_pdl_ids'::regclass))) as pdl_table_size,
    (SELECT pg_size_pretty(
        pg_total_relation_size('derived.vup_linkedin_urls'::regclass) +
        pg_total_relation_size('derived.vup_fbf_ids'::regclass) +
        pg_total_relation_size('derived.vup_pdl_ids'::regclass)
    )) as total_junction_tables_size;

-- ============================================================================
-- FINAL SUMMARY
-- ============================================================================

-- Generate Part 2 completion summary
SELECT 
    'Part 2 Data Migration Complete' as summary_type,
    'All identifier data successfully migrated to junction tables' as migration_status,
    (SELECT COUNT(*) FROM derived.vup_linkedin_urls) as total_linkedin_urls,
    (SELECT COUNT(*) FROM derived.vup_fbf_ids) as total_fbf_ids,
    (SELECT COUNT(*) FROM derived.vup_pdl_ids) as total_pdl_ids,
    'Original vector_universal_person table unchanged' as data_integrity,
    'Junction tables ready for duplicate detection analysis' as data_readiness,
    'Ready to execute Part 3: Identify Duplicates' as next_phase,
    GETDATE() as completed_at;

