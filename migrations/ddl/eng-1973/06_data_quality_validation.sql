-- ============================================================================
-- ENG-1973 Part 6: Data Quality Validation
-- Purpose: Comprehensive validation of migrated data quality
-- ============================================================================

-- Validation 1: Verify (hem, vup_id) uniqueness constraint is enforced
SELECT 
    'Validation 1: (hem, vup_id) uniqueness' as validation_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT (hem, vup_id)) as unique_pairs,
    COUNT(*) - COUNT(DISTINCT (hem, vup_id)) as duplicate_pairs,
    CASE 
        WHEN COUNT(*) = COUNT(DISTINCT (hem, vup_id))
        THEN '✓ PASS'
        ELSE '✗ FAIL - Duplicates detected'
    END as result
FROM derived.vector_email_new
WHERE vup_id IS NOT NULL;

-- Validation 2: Verify cleartext emails are consistently preserved
-- Check that no cleartext email was replaced with hash-only version
WITH original_cleartext AS (
    SELECT DISTINCT sha256
    FROM derived.vector_email
    WHERE email IS NOT NULL
),
migrated_cleartext AS (
    SELECT DISTINCT hem
    FROM derived.vector_email_new
    WHERE email IS NOT NULL
)
SELECT 
    'Validation 2: Cleartext email preservation' as validation_name,
    (SELECT COUNT(*) FROM original_cleartext) as original_cleartext_hems,
    (SELECT COUNT(*) FROM migrated_cleartext) as migrated_cleartext_hems,
    (SELECT COUNT(*) FROM original_cleartext) - (SELECT COUNT(*) FROM migrated_cleartext) as cleartext_hems_lost,
    CASE 
        WHEN (SELECT COUNT(*) FROM migrated_cleartext) >= (SELECT COUNT(*) FROM original_cleartext)
        THEN '✓ PASS'
        WHEN (SELECT COUNT(*) FROM original_cleartext) - (SELECT COUNT(*) FROM migrated_cleartext) < 100
        THEN '⚠ WARNING - Minor cleartext loss (< 100 HEMs)'
        ELSE '✗ FAIL - Significant cleartext loss'
    END as result;

-- Validation 3: Verify domain extraction is correct
SELECT 
    'Validation 3: Domain extraction accuracy' as validation_name,
    COUNT(*) as records_with_email,
    COUNT(domain) as records_with_domain,
    COUNT(*) - COUNT(domain) as missing_domains,
    COUNT(CASE WHEN domain = '' THEN 1 END) as empty_domains,
    COUNT(CASE WHEN domain LIKE '%@%' THEN 1 END) as malformed_domains,
    CASE 
        WHEN COUNT(*) = COUNT(domain) 
             AND COUNT(CASE WHEN domain = '' THEN 1 END) = 0
             AND COUNT(CASE WHEN domain LIKE '%@%' THEN 1 END) = 0
        THEN '✓ PASS'
        ELSE '✗ FAIL - Domain extraction issues detected'
    END as result
FROM derived.vector_email_new
WHERE email IS NOT NULL;

-- Sample records with potential domain extraction issues
SELECT 
    hem,
    email,
    domain,
    'Missing domain' as issue
FROM derived.vector_email_new
WHERE email IS NOT NULL AND domain IS NULL
LIMIT 20

UNION ALL

SELECT 
    hem,
    email,
    domain,
    'Empty domain' as issue
FROM derived.vector_email_new
WHERE email IS NOT NULL AND domain = ''
LIMIT 20

UNION ALL

SELECT 
    hem,
    email,
    domain,
    'Malformed domain (contains @)' as issue
FROM derived.vector_email_new
WHERE email IS NOT NULL AND domain LIKE '%@%'
LIMIT 20;

-- Validation 4: Verify all required fields are populated
SELECT 
    'Validation 4: Required fields completeness' as validation_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN hem IS NULL THEN 1 END) as missing_hem,
    COUNT(CASE WHEN data_source IS NULL OR data_source = '' THEN 1 END) as missing_data_source,
    CASE 
        WHEN COUNT(CASE WHEN hem IS NULL THEN 1 END) = 0
             AND COUNT(CASE WHEN data_source IS NULL OR data_source = '' THEN 1 END) = 0
        THEN '✓ PASS'
        ELSE '✗ FAIL - Required fields are missing'
    END as result
FROM derived.vector_email_new;

-- Validation 5: Verify HEM format (SHA256 is 64 hex characters)
SELECT 
    'Validation 5: HEM hash format validation' as validation_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN LENGTH(hem) != 64 THEN 1 END) as invalid_length,
    COUNT(CASE WHEN hem !~ '^[a-f0-9]{64}$' THEN 1 END) as invalid_format,
    CASE 
        WHEN COUNT(CASE WHEN LENGTH(hem) != 64 THEN 1 END) = 0
             AND COUNT(CASE WHEN hem !~ '^[a-f0-9]{64}$' THEN 1 END) = 0
        THEN '✓ PASS'
        ELSE '✗ FAIL - Invalid HEM formats detected'
    END as result
FROM derived.vector_email_new;

-- Sample records with invalid HEM format
SELECT 
    hem,
    LENGTH(hem) as hem_length,
    email,
    vup_id,
    data_source
FROM derived.vector_email_new
WHERE LENGTH(hem) != 64 OR hem !~ '^[a-f0-9]{64}$'
LIMIT 50;

-- Overall validation summary
SELECT 
    'Overall Validation Summary' as summary,
    (SELECT COUNT(*) FROM derived.vector_email) as original_record_count,
    (SELECT COUNT(*) FROM derived.vector_email_new) as migrated_record_count,
    (SELECT COUNT(*) FROM derived.vector_email) - (SELECT COUNT(*) FROM derived.vector_email_new) as records_deduplicated,
    ROUND(
        100.0 * ((SELECT COUNT(*) FROM derived.vector_email) - (SELECT COUNT(*) FROM derived.vector_email_new))
        / (SELECT COUNT(*) FROM derived.vector_email),
        2
    ) as deduplication_percentage,
    (SELECT COUNT(DISTINCT hem) FROM derived.vector_email_new) as unique_hems,
    (SELECT COUNT(DISTINCT vup_id) FROM derived.vector_email_new WHERE vup_id IS NOT NULL) as unique_vups,
    (SELECT COUNT(*) FROM derived.vector_email_new WHERE email IS NOT NULL) as cleartext_records,
    (SELECT COUNT(*) FROM derived.vector_email_new WHERE email IS NULL) as hash_only_records,
    ROUND(
        100.0 * (SELECT COUNT(*) FROM derived.vector_email_new WHERE email IS NOT NULL)
        / (SELECT COUNT(*) FROM derived.vector_email_new),
        2
    ) as cleartext_percentage;

-- Record validation completion
INSERT INTO derived.vector_email_migration_metadata_eng1973 
    (migration_step, records_affected, notes)
VALUES 
    ('data_quality_validation', 
     (SELECT COUNT(*) FROM derived.vector_email_new),
     'Completed 5 data quality validation checks: uniqueness, cleartext preservation, domain extraction, required fields, HEM format');

-- Display all validation results
SELECT * FROM derived.vector_email_migration_metadata_eng1973
ORDER BY execution_timestamp;
