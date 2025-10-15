-- ============================================================================
-- ENG-1973 Part 1: Analyze Current Data Quality Issues
-- Purpose: Identify data quality problems that the new schema will resolve
-- ============================================================================

-- Issue 1: HEMs with multiple VUPs (canonical VUP ambiguity)
SELECT 
    'HEMs with multiple VUPs' as issue,
    COUNT(*) as affected_hems,
    SUM(vup_count) as total_conflicting_records
FROM (
    SELECT 
        sha256,
        COUNT(DISTINCT vup_id) as vup_count
    FROM derived.vector_email
    WHERE vup_id IS NOT NULL
    GROUP BY sha256
    HAVING COUNT(DISTINCT vup_id) > 1
) conflicts;

-- Sample of conflicting HEMs for review
SELECT 
    sha256,
    LISTAGG(DISTINCT vup_id, ', ') WITHIN GROUP (ORDER BY vup_id) as vup_ids,
    COUNT(*) as record_count,
    COUNT(CASE WHEN email IS NOT NULL THEN 1 END) as with_cleartext,
    LISTAGG(DISTINCT email_type, ', ') WITHIN GROUP (ORDER BY email_type) as email_types,
    LISTAGG(DISTINCT data_source, ', ') WITHIN GROUP (ORDER BY data_source) as data_sources
FROM derived.vector_email
WHERE sha256 IN (
    SELECT sha256
    FROM derived.vector_email
    WHERE vup_id IS NOT NULL
    GROUP BY sha256
    HAVING COUNT(DISTINCT vup_id) > 1
    LIMIT 100
)
GROUP BY sha256
ORDER BY record_count DESC;

-- Issue 2: Same HEM with both cleartext and hash-only records
SELECT 
    'HEMs with mixed cleartext/hash-only records' as issue,
    COUNT(DISTINCT sha256) as affected_hems
FROM (
    SELECT sha256
    FROM derived.vector_email
    GROUP BY sha256
    HAVING 
        COUNT(CASE WHEN email IS NOT NULL THEN 1 END) > 0
        AND COUNT(CASE WHEN email IS NULL THEN 1 END) > 0
);

-- Sample mixed cleartext/hash-only HEMs
SELECT 
    sha256,
    COUNT(*) as total_records,
    COUNT(CASE WHEN email IS NOT NULL THEN 1 END) as with_cleartext,
    COUNT(CASE WHEN email IS NULL THEN 1 END) as hash_only,
    LISTAGG(DISTINCT CASE WHEN email IS NOT NULL THEN 'cleartext' ELSE 'hash-only' END, ', ') 
        WITHIN GROUP (ORDER BY email) as record_types
FROM derived.vector_email
WHERE sha256 IN (
    SELECT sha256
    FROM derived.vector_email
    GROUP BY sha256
    HAVING 
        COUNT(CASE WHEN email IS NOT NULL THEN 1 END) > 0
        AND COUNT(CASE WHEN email IS NULL THEN 1 END) > 0
    LIMIT 50
)
GROUP BY sha256;

-- Issue 3: Duplicate rows (exact duplicates across all columns)
WITH duplicate_check AS (
    SELECT 
        vup_id,
        sha256,
        email,
        email_type,
        data_source,
        dataset_version,
        COUNT(*) as duplicate_count
    FROM derived.vector_email
    GROUP BY vup_id, sha256, email, email_type, data_source, dataset_version
    HAVING COUNT(*) > 1
)
SELECT 
    'Exact duplicate rows' as issue,
    COUNT(*) as duplicate_groups,
    SUM(duplicate_count) as total_duplicate_records,
    SUM(duplicate_count) - COUNT(*) as records_to_remove
FROM duplicate_check;

-- Sample exact duplicates
SELECT 
    vup_id,
    sha256,
    email,
    email_type,
    data_source,
    COUNT(*) as occurrence_count
FROM derived.vector_email
GROUP BY vup_id, sha256, email, email_type, data_source, dataset_version
HAVING COUNT(*) > 1
LIMIT 20;

-- Issue 4: VUPs with multiple business emails (canonical email ambiguity)
SELECT 
    'VUPs with multiple business emails' as issue,
    COUNT(*) as affected_vups,
    AVG(email_count) as avg_business_emails_per_vup,
    MAX(email_count) as max_business_emails_per_vup
FROM (
    SELECT 
        vup_id,
        COUNT(DISTINCT email) as email_count
    FROM derived.vector_email
    WHERE email_type = 'business'
      AND email IS NOT NULL
      AND vup_id IS NOT NULL
    GROUP BY vup_id
    HAVING COUNT(DISTINCT email) > 1
);

-- Sample VUPs with multiple business emails
SELECT 
    vup_id,
    COUNT(DISTINCT email) as business_email_count,
    LISTAGG(DISTINCT email, ' | ') WITHIN GROUP (ORDER BY email) as business_emails,
    LISTAGG(DISTINCT data_source, ', ') WITHIN GROUP (ORDER BY data_source) as sources
FROM derived.vector_email
WHERE email_type = 'business'
  AND email IS NOT NULL
  AND vup_id IS NOT NULL
GROUP BY vup_id
HAVING COUNT(DISTINCT email) > 1
LIMIT 20;

-- Data source distribution analysis
SELECT 
    data_source,
    COUNT(*) as record_count,
    COUNT(DISTINCT sha256) as unique_hems,
    COUNT(DISTINCT vup_id) as unique_vups,
    COUNT(CASE WHEN email IS NOT NULL THEN 1 END) as with_cleartext,
    COUNT(CASE WHEN email IS NULL THEN 1 END) as hash_only,
    ROUND(100.0 * COUNT(CASE WHEN email IS NOT NULL THEN 1 END) / COUNT(*), 2) as pct_cleartext
FROM derived.vector_email
GROUP BY data_source
ORDER BY record_count DESC;

-- Email type distribution analysis
SELECT 
    email_type,
    COUNT(*) as record_count,
    COUNT(CASE WHEN vup_id IS NOT NULL THEN 1 END) as with_vup_id,
    COUNT(CASE WHEN email IS NOT NULL THEN 1 END) as with_cleartext,
    COUNT(DISTINCT sha256) as unique_hems,
    ROUND(100.0 * COUNT(CASE WHEN email IS NOT NULL THEN 1 END) / COUNT(*), 2) as pct_cleartext
FROM derived.vector_email
GROUP BY email_type
ORDER BY record_count DESC;

-- (hem, vup_id) uniqueness check - THIS IS THE KEY CONSTRAINT VIOLATION
WITH hem_vup_duplicates AS (
    SELECT 
        sha256,
        vup_id,
        COUNT(*) as duplicate_count
    FROM derived.vector_email
    WHERE vup_id IS NOT NULL
    GROUP BY sha256, vup_id
    HAVING COUNT(*) > 1
)
SELECT 
    '(hem, vup_id) duplicate pairs' as issue,
    COUNT(*) as duplicate_pair_count,
    SUM(duplicate_count) as total_records_affected,
    SUM(duplicate_count) - COUNT(*) as records_requiring_deduplication
FROM hem_vup_duplicates;

-- Sample (hem, vup_id) duplicates
SELECT 
    sha256,
    vup_id,
    COUNT(*) as occurrence_count,
    LISTAGG(DISTINCT email, ' | ') WITHIN GROUP (ORDER BY email) as emails,
    LISTAGG(DISTINCT email_type, ', ') WITHIN GROUP (ORDER BY email_type) as email_types,
    LISTAGG(DISTINCT data_source, ', ') WITHIN GROUP (ORDER BY data_source) as data_sources
FROM derived.vector_email
WHERE vup_id IS NOT NULL
GROUP BY sha256, vup_id
HAVING COUNT(*) > 1
LIMIT 50;

-- Summary statistics
SELECT 
    'Total Records' as metric,
    COUNT(*) as value
FROM derived.vector_email

UNION ALL

SELECT 
    'Unique HEMs',
    COUNT(DISTINCT sha256)
FROM derived.vector_email

UNION ALL

SELECT 
    'Unique VUPs',
    COUNT(DISTINCT vup_id)
FROM derived.vector_email
WHERE vup_id IS NOT NULL

UNION ALL

SELECT 
    'Records with cleartext',
    COUNT(*)
FROM derived.vector_email
WHERE email IS NOT NULL

UNION ALL

SELECT 
    'Hash-only records',
    COUNT(*)
FROM derived.vector_email
WHERE email IS NULL

UNION ALL

SELECT 
    'Unique (hem, vup_id) pairs',
    COUNT(DISTINCT (sha256, vup_id))
FROM derived.vector_email
WHERE vup_id IS NOT NULL;

-- Expected deduplication impact
SELECT 
    'Expected migration impact' as summary,
    (SELECT COUNT(*) FROM derived.vector_email) as current_total_records,
    (SELECT COUNT(DISTINCT (sha256, vup_id)) FROM derived.vector_email WHERE vup_id IS NOT NULL) as expected_hem_vup_records,
    (SELECT COUNT(DISTINCT sha256) FROM derived.vector_email WHERE vup_id IS NULL) as expected_hem_only_records,
    (SELECT COUNT(DISTINCT (sha256, vup_id)) FROM derived.vector_email WHERE vup_id IS NOT NULL) +
    (SELECT COUNT(DISTINCT sha256) FROM derived.vector_email WHERE vup_id IS NULL) as expected_total_new_records,
    (SELECT COUNT(*) FROM derived.vector_email) - 
    ((SELECT COUNT(DISTINCT (sha256, vup_id)) FROM derived.vector_email WHERE vup_id IS NOT NULL) +
     (SELECT COUNT(DISTINCT sha256) FROM derived.vector_email WHERE vup_id IS NULL)) as records_to_deduplicate,
    ROUND(100.0 * ((SELECT COUNT(*) FROM derived.vector_email) - 
    ((SELECT COUNT(DISTINCT (sha256, vup_id)) FROM derived.vector_email WHERE vup_id IS NOT NULL) +
     (SELECT COUNT(DISTINCT sha256) FROM derived.vector_email WHERE vup_id IS NULL))) / 
    (SELECT COUNT(*) FROM derived.vector_email), 2) as deduplication_percentage;
