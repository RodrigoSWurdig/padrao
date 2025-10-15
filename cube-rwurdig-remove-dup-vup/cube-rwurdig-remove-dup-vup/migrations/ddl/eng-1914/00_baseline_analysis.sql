-- ============================================================================
-- ENG-1914 Baseline Analysis: Establish Pre-Migration Metrics
-- Purpose: Document current state for comparison during verification phases
-- Prerequisites: Phase 0 cleanup completed with zero orphaned records
-- Output: Document all results in baseline_results.md for reference
-- ============================================================================

-- Section 1: Person table core statistics
SELECT 
    'Person Table Baseline' as metric_category,
    COUNT(*) as total_person_records,
    COUNT(DISTINCT vup_id) as unique_vup_ids,
    COUNT(*) - COUNT(DISTINCT vup_id) as vup_id_duplicates,
    COUNT(CASE WHEN merged_into_vup_id IS NOT NULL THEN 1 END) as pre_existing_merged_records,
    pg_size_pretty(pg_total_relation_size('derived.vector_universal_person'::regclass)) as table_size,
    GETDATE() as measured_at
FROM derived.vector_universal_person;

-- Section 2: Identifier population analysis
SELECT 
    'Identifier Population Rates' as metric_category,
    COUNT(*) as total_records,
    COUNT(CASE WHEN linkedin_url IS NOT NULL AND TRIM(linkedin_url) != '' THEN 1 END) as linkedin_populated,
    COUNT(CASE WHEN fbf_id IS NOT NULL AND TRIM(fbf_id) != '' THEN 1 END) as fbf_populated,
    COUNT(CASE WHEN pdl_id IS NOT NULL AND TRIM(pdl_id) != '' THEN 1 END) as pdl_populated,
    ROUND(100.0 * COUNT(CASE WHEN linkedin_url IS NOT NULL AND TRIM(linkedin_url) != '' THEN 1 END) / NULLIF(COUNT(*), 0), 2) as pct_linkedin,
    ROUND(100.0 * COUNT(CASE WHEN fbf_id IS NOT NULL AND TRIM(fbf_id) != '' THEN 1 END) / NULLIF(COUNT(*), 0), 2) as pct_fbf,
    ROUND(100.0 * COUNT(CASE WHEN pdl_id IS NOT NULL AND TRIM(pdl_id) != '' THEN 1 END) / NULLIF(COUNT(*), 0), 2) as pct_pdl
FROM derived.vector_universal_person;

-- Section 3: Identifier uniqueness analysis
SELECT 
    'LinkedIn URL Duplicates' as identifier_type,
    COUNT(*) as total_populated,
    COUNT(DISTINCT linkedin_url) as unique_values,
    COUNT(*) - COUNT(DISTINCT linkedin_url) as duplicate_instances,
    ROUND(100.0 * (COUNT(*) - COUNT(DISTINCT linkedin_url)) / NULLIF(COUNT(*), 0), 4) as duplicate_pct
FROM derived.vector_universal_person
WHERE linkedin_url IS NOT NULL AND TRIM(linkedin_url) != ''

UNION ALL

SELECT 
    'FBF ID Duplicates',
    COUNT(*),
    COUNT(DISTINCT fbf_id),
    COUNT(*) - COUNT(DISTINCT fbf_id),
    ROUND(100.0 * (COUNT(*) - COUNT(DISTINCT fbf_id)) / NULLIF(COUNT(*), 0), 4)
FROM derived.vector_universal_person
WHERE fbf_id IS NOT NULL AND TRIM(fbf_id) != ''

UNION ALL

SELECT 
    'PDL ID Duplicates',
    COUNT(*),
    COUNT(DISTINCT pdl_id),
    COUNT(*) - COUNT(DISTINCT pdl_id),
    ROUND(100.0 * (COUNT(*) - COUNT(DISTINCT pdl_id)) / NULLIF(COUNT(*), 0), 4)
FROM derived.vector_universal_person
WHERE pdl_id IS NOT NULL AND TRIM(pdl_id) != '';

-- Section 4: Email HEM analysis from vector_email
SELECT 
    'Email HEM Duplicates' as metric_category,
    COUNT(*) as total_email_records,
    COUNT(DISTINCT sha256) as unique_hems,
    COUNT(*) - COUNT(DISTINCT sha256) as duplicate_hem_instances,
    COUNT(DISTINCT vup_id) as unique_persons_with_email,
    COUNT(DISTINCT vup_id || '::' || sha256) as unique_vup_hem_pairs,
    ROUND(100.0 * (COUNT(*) - COUNT(DISTINCT vup_id || '::' || sha256)) / NULLIF(COUNT(*), 0), 4) as duplicate_vup_hem_pct
FROM derived.vector_email
WHERE sha256 IS NOT NULL 
  AND TRIM(sha256) != ''
  AND LENGTH(TRIM(sha256)) = 64
  AND vup_id IS NOT NULL;

-- Section 5: Sample high-duplication identifiers for reference
SELECT 
    'High-Duplication LinkedIn URLs' as sample_type,
    linkedin_url,
    COUNT(*) as person_count,
    LISTAGG(DISTINCT SUBSTRING(vup_id, 1, 8), ', ') WITHIN GROUP (ORDER BY vup_id) as sample_vup_id_prefixes
FROM derived.vector_universal_person
WHERE linkedin_url IS NOT NULL AND TRIM(linkedin_url) != ''
GROUP BY linkedin_url
HAVING COUNT(*) >= 5
ORDER BY COUNT(*) DESC
LIMIT 10;

-- Section 6: Child table relationship counts
SELECT 
    'Child Table Baseline' as metric_category,
    (SELECT COUNT(*) FROM derived.vector_universal_job) as total_job_records,
    (SELECT COUNT(DISTINCT vup_id) FROM derived.vector_universal_job) as persons_with_jobs,
    (SELECT COUNT(*) FROM derived.vector_email WHERE vup_id IS NOT NULL) as total_email_records_linked,
    (SELECT COUNT(DISTINCT vup_id) FROM derived.vector_email WHERE vup_id IS NOT NULL) as persons_with_emails,
    (SELECT pg_size_pretty(pg_total_relation_size('derived.vector_universal_job'::regclass))) as jobs_table_size,
    (SELECT pg_size_pretty(pg_total_relation_size('derived.vector_email'::regclass))) as emails_table_size;

-- Section 7: Orphan verification (should be zero after Phase 0)
SELECT 
    'Orphan Verification' as validation_type,
    (SELECT COUNT(*) 
     FROM derived.vector_universal_job vuj 
     LEFT JOIN derived.vector_universal_person vup ON vuj.vup_id = vup.vup_id
     WHERE vup.vup_id IS NULL) as orphaned_jobs,
    (SELECT COUNT(*) 
     FROM derived.vector_email ve 
     LEFT JOIN derived.vector_universal_person vup ON ve.vup_id = vup.vup_id
     WHERE vup.vup_id IS NULL AND ve.vup_id IS NOT NULL) as orphaned_emails,
    CASE 
        WHEN (SELECT COUNT(*) 
              FROM derived.vector_universal_job vuj 
              LEFT JOIN derived.vector_universal_person vup ON vuj.vup_id = vup.vup_id
              WHERE vup.vup_id IS NULL) = 0
         AND (SELECT COUNT(*) 
              FROM derived.vector_email ve 
              LEFT JOIN derived.vector_universal_person vup ON ve.vup_id = vup.vup_id
              WHERE vup.vup_id IS NULL AND ve.vup_id IS NOT NULL) = 0
        THEN 'VERIFIED CLEAN - Ready for Part 1'
        ELSE 'ORPHANS DETECTED - Return to Phase 0'
    END as validation_status;

-- Section 8: Data quality metrics for winner selection logic
SELECT 
    'Person Completeness Distribution' as metric_category,
    CASE 
        WHEN job_count = 0 THEN '0 jobs'
        WHEN job_count BETWEEN 1 AND 2 THEN '1-2 jobs'
        WHEN job_count BETWEEN 3 AND 5 THEN '3-5 jobs'
        WHEN job_count BETWEEN 6 AND 10 THEN '6-10 jobs'
        ELSE '11+ jobs'
    END as job_count_bucket,
    COUNT(*) as person_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as pct_of_persons
FROM (
    SELECT 
        vup.vup_id,
        COUNT(vuj.vup_id) as job_count
    FROM derived.vector_universal_person vup
    LEFT JOIN derived.vector_universal_job vuj ON vup.vup_id = vuj.vup_id
    GROUP BY vup.vup_id
) person_jobs
GROUP BY 
    CASE 
        WHEN job_count = 0 THEN '0 jobs'
        WHEN job_count BETWEEN 1 AND 2 THEN '1-2 jobs'
        WHEN job_count BETWEEN 3 AND 5 THEN '3-5 jobs'
        WHEN job_count BETWEEN 6 AND 10 THEN '6-10 jobs'
        ELSE '11+ jobs'
    END
ORDER BY MIN(job_count);

-- Section 9: Update recency distribution
SELECT 
    'Person Update Recency' as metric_category,
    CASE 
        WHEN updated_at >= DATEADD(day, -30, GETDATE()) THEN 'Last 30 days'
        WHEN updated_at >= DATEADD(day, -90, GETDATE()) THEN '30-90 days ago'
        WHEN updated_at >= DATEADD(day, -180, GETDATE()) THEN '90-180 days ago'
        WHEN updated_at >= DATEADD(year, -1, GETDATE()) THEN '180 days - 1 year ago'
        ELSE 'Over 1 year ago'
    END as recency_bucket,
    COUNT(*) as person_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as pct_of_persons
FROM derived.vector_universal_person
GROUP BY 
    CASE 
        WHEN updated_at >= DATEADD(day, -30, GETDATE()) THEN 'Last 30 days'
        WHEN updated_at >= DATEADD(day, -90, GETDATE()) THEN '30-90 days ago'
        WHEN updated_at >= DATEADD(day, -180, GETDATE()) THEN '90-180 days ago'
        WHEN updated_at >= DATEADD(year, -1, GETDATE()) THEN '180 days - 1 year ago'
        ELSE 'Over 1 year ago'
    END
ORDER BY MIN(updated_at) DESC;

-- Final baseline summary
SELECT 
    'Baseline Analysis Complete' as summary_status,
    (SELECT COUNT(*) FROM derived.vector_universal_person) as total_persons,
    (SELECT COUNT(*) FROM derived.vector_universal_job) as total_jobs,
    (SELECT COUNT(*) FROM derived.vector_email WHERE vup_id IS NOT NULL) as total_emails,
    'Document all results in baseline_results.md' as next_action,
    'Proceed to executing Part 1 infrastructure creation' as next_phase,
    GETDATE() as analysis_completed_at;
