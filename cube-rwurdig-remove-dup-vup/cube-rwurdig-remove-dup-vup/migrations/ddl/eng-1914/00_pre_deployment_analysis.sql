-- ============================================================================
-- ENG-1914 Part 0: Pre-Deployment Analysis Queries
-- Execute in DEV environment to establish baseline metrics
-- Document all results in migrations/documentation/eng-1914-analysis-results.md
-- ============================================================================

-- ============================================================================
-- SECTION 1: DATA VOLUME ASSESSMENT
-- ============================================================================

-- Query 1A: Current Person Table Volume and Identifier Population
SELECT 
    'Current Data Volume' as analysis_type,
    COUNT(*) as total_person_records,
    COUNT(CASE WHEN linkedin_url IS NOT NULL AND TRIM(linkedin_url) != '' THEN 1 END) as linkedin_urls_populated,
    COUNT(CASE WHEN fbf_id IS NOT NULL AND TRIM(fbf_id) != '' THEN 1 END) as fbf_ids_populated,
    COUNT(CASE WHEN pdl_id IS NOT NULL AND TRIM(pdl_id) != '' THEN 1 END) as pdl_ids_populated,
    ROUND(100.0 * COUNT(CASE WHEN linkedin_url IS NOT NULL AND TRIM(linkedin_url) != '' THEN 1 END) / COUNT(*), 2) as pct_with_linkedin,
    ROUND(100.0 * COUNT(CASE WHEN fbf_id IS NOT NULL AND TRIM(fbf_id) != '' THEN 1 END) / COUNT(*), 2) as pct_with_fbf,
    ROUND(100.0 * COUNT(CASE WHEN pdl_id IS NOT NULL AND TRIM(pdl_id) != '' THEN 1 END) / COUNT(*), 2) as pct_with_pdl
FROM derived.vector_universal_person;

-- Query 1B: Identifier Uniqueness and Duplication Patterns
WITH identifier_counts AS (
    SELECT 
        'LinkedIn URL' as identifier_type,
        COUNT(DISTINCT linkedin_url) as unique_identifiers,
        COUNT(*) as total_records,
        COUNT(*) - COUNT(DISTINCT linkedin_url) as potential_duplicates
    FROM derived.vector_universal_person
    WHERE linkedin_url IS NOT NULL AND TRIM(linkedin_url) != ''
    
    UNION ALL
    
    SELECT 
        'FBF ID' as identifier_type,
        COUNT(DISTINCT fbf_id) as unique_identifiers,
        COUNT(*) as total_records,
        COUNT(*) - COUNT(DISTINCT fbf_id) as potential_duplicates
    FROM derived.vector_universal_person
    WHERE fbf_id IS NOT NULL AND TRIM(fbf_id) != ''
    
    UNION ALL
    
    SELECT 
        'PDL ID' as identifier_type,
        COUNT(DISTINCT pdl_id) as unique_identifiers,
        COUNT(*) as total_records,
        COUNT(*) - COUNT(DISTINCT pdl_id) as potential_duplicates
    FROM derived.vector_universal_person
    WHERE pdl_id IS NOT NULL AND TRIM(pdl_id) != ''
)
SELECT 
    identifier_type,
    unique_identifiers,
    total_records,
    potential_duplicates,
    ROUND(100.0 * potential_duplicates / NULLIF(total_records, 0), 2) as duplicate_percentage
FROM identifier_counts
ORDER BY potential_duplicates DESC;

-- ============================================================================
-- SECTION 2: CHILD TABLE IMPACT ASSESSMENT
-- ============================================================================

-- Query 2A: Complete Child Table Relationships
SELECT 
    'Child Tables Assessment' as analysis_type,
    (SELECT COUNT(*) FROM derived.vector_universal_job) as total_job_records,
    (SELECT COUNT(DISTINCT vup_id) FROM derived.vector_universal_job) as unique_persons_with_jobs,
    (SELECT ROUND(AVG(job_count), 2) FROM (SELECT vup_id, COUNT(*) as job_count FROM derived.vector_universal_job GROUP BY vup_id) x) as avg_jobs_per_person,
    (SELECT MAX(job_count) FROM (SELECT vup_id, COUNT(*) as job_count FROM derived.vector_universal_job GROUP BY vup_id) x) as max_jobs_for_any_person,
    (SELECT COUNT(*) FROM derived.vector_email) as total_email_records,
    (SELECT COUNT(DISTINCT vup_id) FROM derived.vector_email WHERE vup_id IS NOT NULL) as unique_persons_with_emails,
    (SELECT COUNT(*) FROM derived.vector_maid) as total_maid_records,
    (SELECT COUNT(DISTINCT vup_id) FROM derived.vector_maid) as unique_persons_with_maids,
    (SELECT COUNT(*) FROM derived.vector_phone) as total_phone_records,
    (SELECT COUNT(DISTINCT vup_id) FROM derived.vector_phone) as unique_persons_with_phones;

-- Query 2B: Child Table Density Analysis
SELECT 
    'Child Table Density' as analysis_type,
    ROUND(100.0 * (SELECT COUNT(DISTINCT vup_id) FROM derived.vector_universal_job) / (SELECT COUNT(*) FROM derived.vector_universal_person), 2) as pct_persons_with_jobs,
    ROUND(100.0 * (SELECT COUNT(DISTINCT vup_id) FROM derived.vector_email WHERE vup_id IS NOT NULL) / (SELECT COUNT(*) FROM derived.vector_universal_person), 2) as pct_persons_with_emails,
    ROUND(100.0 * (SELECT COUNT(DISTINCT vup_id) FROM derived.vector_maid) / (SELECT COUNT(*) FROM derived.vector_universal_person), 2) as pct_persons_with_maids,
    ROUND(100.0 * (SELECT COUNT(DISTINCT vup_id) FROM derived.vector_phone) / (SELECT COUNT(*) FROM derived.vector_universal_person), 2) as pct_persons_with_phones;

-- ============================================================================
-- SECTION 3: STORAGE AND PERFORMANCE METRICS
-- ============================================================================

-- Query 3A: Current Storage Utilization
SELECT 
    'Storage Analysis' as analysis_type,
    pg_size_pretty(pg_total_relation_size('derived.vector_universal_person')) as current_table_size,
    pg_size_pretty(pg_relation_size('derived.vector_universal_person')) as current_data_size,
    pg_size_pretty(pg_total_relation_size('derived.vector_universal_person') - pg_relation_size('derived.vector_universal_person')) as current_index_size;

-- Query 3B: Child Table Storage
SELECT 
    'Child Table Storage' as analysis_type,
    pg_size_pretty(pg_total_relation_size('derived.vector_universal_job')) as jobs_table_size,
    pg_size_pretty(pg_total_relation_size('derived.vector_email')) as emails_table_size,
    pg_size_pretty(pg_total_relation_size('derived.vector_maid')) as maids_table_size,
    pg_size_pretty(pg_total_relation_size('derived.vector_phone')) as phones_table_size;

-- ============================================================================
-- SECTION 4: DUPLICATE PATTERN SAMPLING
-- ============================================================================

-- Query 4A: Sample LinkedIn URL Duplicates
WITH linkedin_dupes AS (
    SELECT 
        linkedin_url,
        COUNT(*) as vup_count,
        LISTAGG(DISTINCT vup_id, ', ') WITHIN GROUP (ORDER BY vup_id) as sample_vup_ids
    FROM derived.vector_universal_person
    WHERE linkedin_url IS NOT NULL AND TRIM(linkedin_url) != ''
    GROUP BY linkedin_url
    HAVING COUNT(*) > 1
    ORDER BY vup_count DESC
    LIMIT 10
)
SELECT 
    'Sample LinkedIn URL Duplicates' as duplicate_type,
    linkedin_url,
    vup_count,
    sample_vup_ids
FROM linkedin_dupes
ORDER BY vup_count DESC;

-- Query 4B: Sample FBF ID Duplicates
WITH fbf_dupes AS (
    SELECT 
        fbf_id,
        COUNT(*) as vup_count,
        LISTAGG(DISTINCT vup_id, ', ') WITHIN GROUP (ORDER BY vup_id) as sample_vup_ids
    FROM derived.vector_universal_person
    WHERE fbf_id IS NOT NULL AND TRIM(fbf_id) != ''
    GROUP BY fbf_id
    HAVING COUNT(*) > 1
    ORDER BY vup_count DESC
    LIMIT 10
)
SELECT 
    'Sample FBF ID Duplicates' as duplicate_type,
    fbf_id,
    vup_count,
    sample_vup_ids
FROM fbf_dupes
ORDER BY vup_count DESC;

-- Query 4C: Sample PDL ID Duplicates
WITH pdl_dupes AS (
    SELECT 
        pdl_id,
        COUNT(*) as vup_count,
        LISTAGG(DISTINCT vup_id, ', ') WITHIN GROUP (ORDER BY vup_id) as sample_vup_ids
    FROM derived.vector_universal_person
    WHERE pdl_id IS NOT NULL AND TRIM(pdl_id) != ''
    GROUP BY pdl_id
    HAVING COUNT(*) > 1
    LIMIT 10
)
SELECT 
    'Sample PDL ID Duplicates' as duplicate_type,
    pdl_id,
    vup_count,
    sample_vup_ids
FROM pdl_dupes
ORDER BY vup_count DESC;

-- ============================================================================
-- SECTION 5: DATASHARE CONFIGURATION VERIFICATION
-- ============================================================================

-- Query 5A: Verify Current Datashare Membership
SELECT 
    'Datashare Membership' as analysis_type,
    object_name,
    object_type,
    share_name,
    share_type
FROM svv_datashare_objects
WHERE share_name = 'vector_core_datashare'
  AND schema = 'derived'
  AND object_name LIKE 'vector%'
ORDER BY object_name;

-- Query 5B: Verify Consumer Access Configuration
SELECT 
    'Consumer Access' as analysis_type,
    database_name,
    schema_name,
    table_name,
    table_type
FROM svv_external_tables
WHERE schema_name = 'derived'
  AND table_name LIKE 'vector%'
ORDER BY table_name;

-- ============================================================================
-- SECTION 6: DATA QUALITY AND COMPLETENESS CHECKS
-- ============================================================================

-- Query 6A: Person Record Completeness Score
SELECT 
    'Data Completeness' as analysis_type,
    ROUND(AVG(CASE WHEN first_name IS NOT NULL THEN 1 ELSE 0 END) * 100, 2) as pct_with_first_name,
    ROUND(AVG(CASE WHEN last_name IS NOT NULL THEN 1 ELSE 0 END) * 100, 2) as pct_with_last_name,
    ROUND(AVG(CASE WHEN street_address IS NOT NULL THEN 1 ELSE 0 END) * 100, 2) as pct_with_address,
    ROUND(AVG(CASE WHEN locality IS NOT NULL THEN 1 ELSE 0 END) * 100, 2) as pct_with_locality,
    ROUND(AVG(CASE WHEN country IS NOT NULL THEN 1 ELSE 0 END) * 100, 2) as pct_with_country,
    ROUND(AVG(CASE WHEN gender IS NOT NULL THEN 1 ELSE 0 END) * 100, 2) as pct_with_gender
FROM derived.vector_universal_person;

-- Query 6B: Data Source Distribution
SELECT 
    'Data Source Distribution' as analysis_type,
    data_source,
    COUNT(*) as record_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage
FROM derived.vector_universal_person
GROUP BY data_source
ORDER BY record_count DESC;
