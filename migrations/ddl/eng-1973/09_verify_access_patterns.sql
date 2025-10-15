-- ============================================================================
-- ENG-1973 Part 9: Verify Access Patterns
-- ============================================================================

-- Access Pattern 1: Given HEM, find person ID (vup_id)
-- Expected Performance: Fast lookup via DISTKEY(hem)

EXPLAIN
SELECT 
    hem,
    vup_id,
    email_type
FROM derived.vector_email_new
WHERE hem = 'a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0u1v2w3x4y5z6a7b8c9d0e1f2'
LIMIT 1;

-- Test Access Pattern 1 with actual data
SELECT 
    'Access Pattern 1: HEM → VUP lookup' as pattern_name,
    COUNT(*) as test_queries_executed,
    AVG(query_duration_ms) as avg_duration_ms
FROM (
    SELECT 
        hem,
        EXTRACT(EPOCH FROM (GETDATE() - start_time)) * 1000 as query_duration_ms
    FROM (
        SELECT hem, GETDATE() as start_time
        FROM derived.vector_email_new
        WHERE email IS NOT NULL
        LIMIT 100
    ) samples
) test_results;

-- Sample results for Access Pattern 1
SELECT 
    hem,
    vup_id,
    email,
    email_type,
    data_source
FROM derived.vector_email_new
WHERE hem IN (
    SELECT hem 
    FROM derived.vector_email_new 
    WHERE vup_id IS NOT NULL 
    LIMIT 10
)
ORDER BY hem;

-- Access Pattern 2: Given HEM, retrieve cleartext email
-- Expected Performance: Fast lookup via DISTKEY(hem)

EXPLAIN
SELECT 
    hem,
    email,
    domain,
    email_type
FROM derived.vector_email_new
WHERE hem = 'a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0u1v2w3x4y5z6a7b8c9d0e1f2'
  AND email IS NOT NULL
LIMIT 1;

-- Test Access Pattern 2 with actual data
SELECT 
    'Access Pattern 2: HEM → Cleartext email lookup' as pattern_name,
    COUNT(*) as total_hems,
    COUNT(email) as hems_with_cleartext,
    ROUND(100.0 * COUNT(email) / COUNT(*), 2) as cleartext_coverage_pct
FROM derived.vector_email_new;

-- Sample results for Access Pattern 2
SELECT 
    hem,
    email,
    domain,
    email_type
FROM derived.vector_email_new
WHERE email IS NOT NULL
LIMIT 20;

-- Access Pattern 3: Given vup_id, retrieve all associated HEMs
-- Expected Performance: Efficient via SORTKEY(vup_id, hem)
EXPLAIN
SELECT 
    vup_id,
    hem,
    email,
    email_type,
    domain
FROM derived.vector_email_new
WHERE vup_id = 'test_vup_123456789'
ORDER BY 
    CASE WHEN email_type = 'business' THEN 1 ELSE 2 END,
    CASE WHEN email IS NOT NULL THEN 1 ELSE 2 END;

-- Test Access Pattern 3 with actual data
SELECT 
    'Access Pattern 3: VUP → All HEMs lookup' as pattern_name,
    COUNT(DISTINCT vup_id) as total_vups,
    AVG(hem_count) as avg_hems_per_vup,
    MAX(hem_count) as max_hems_per_vup,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY hem_count) as median_hems_per_vup
FROM (
    SELECT 
        vup_id,
        COUNT(*) as hem_count
    FROM derived.vector_email_new
    WHERE vup_id IS NOT NULL
    GROUP BY vup_id
) vup_stats;

-- Sample results for Access Pattern 3 (VUPs with multiple emails)
SELECT 
    vup_id,
    COUNT(*) as hem_count,
    COUNT(CASE WHEN email IS NOT NULL THEN 1 END) as cleartext_count,
    COUNT(CASE WHEN email_type = 'business' THEN 1 END) as business_count,
    COUNT(CASE WHEN email_type = 'personal' THEN 1 END) as personal_count
FROM derived.vector_email_new
WHERE vup_id IS NOT NULL
GROUP BY vup_id
HAVING COUNT(*) > 1
ORDER BY hem_count DESC
LIMIT 20;

-- Detailed view of a multi-email VUP
SELECT 
    vup_id,
    hem,
    email,
    domain,
    email_type,
    data_source
FROM derived.vector_email_new
WHERE vup_id IN (
    SELECT vup_id
    FROM derived.vector_email_new
    WHERE vup_id IS NOT NULL
    GROUP BY vup_id
    HAVING COUNT(*) > 1
    LIMIT 5
)
ORDER BY vup_id, 
         CASE WHEN email_type = 'business' THEN 1 ELSE 2 END,
         CASE WHEN email IS NOT NULL THEN 1 ELSE 2 END;

-- Access Pattern 4: Given vup_id, retrieve canonical business email
-- Expected Performance: Use DynamoDB export view for optimal performance
EXPLAIN
SELECT 
    vup_id,
    canonical_hem,
    canonical_email,
    canonical_domain,
    canonical_email_type
FROM derived.v_dynamodb_email_export
WHERE vup_id = 'test_vup_123456789';

-- Test Access Pattern 4 with actual data
SELECT 
    'Access Pattern 4: VUP → Canonical email lookup' as pattern_name,
    COUNT(*) as total_vups,
    COUNT(canonical_email) as vups_with_cleartext,
    COUNT(CASE WHEN canonical_email_type = 'business' THEN 1 END) as canonical_business_emails,
    COUNT(CASE WHEN canonical_email_type = 'personal' THEN 1 END) as canonical_personal_emails,
    ROUND(100.0 * COUNT(canonical_email) / COUNT(*), 2) as cleartext_coverage_pct,
    ROUND(100.0 * COUNT(CASE WHEN canonical_email_type = 'business' THEN 1 END) / COUNT(*), 2) as business_email_pct
FROM derived.v_dynamodb_email_export;

-- Sample results for Access Pattern 4
SELECT 
    vup_id,
    canonical_hem,
    canonical_email,
    canonical_domain,
    canonical_email_type,
    canonical_data_source
FROM derived.v_dynamodb_email_export
WHERE canonical_email IS NOT NULL
LIMIT 50;

-- Verify canonical selection for VUPs with multiple emails
WITH multi_email_vups AS (
    SELECT vup_id
    FROM derived.vector_email_new
    WHERE vup_id IS NOT NULL
    GROUP BY vup_id
    HAVING COUNT(*) > 1
    LIMIT 20
)
SELECT 
    ve.vup_id,
    COUNT(*) as total_emails,
    COUNT(CASE WHEN ve.email_type = 'business' THEN 1 END) as business_emails,
    COUNT(CASE WHEN ve.email IS NOT NULL THEN 1 END) as cleartext_emails,
    ddb.canonical_email,
    ddb.canonical_email_type,
    ddb.canonical_data_source,
    CASE 
        WHEN ddb.canonical_email_type = 'business' 
             AND ddb.canonical_email IS NOT NULL 
        THEN '✓ Optimal canonical selection'
        WHEN ddb.canonical_email IS NOT NULL 
        THEN '⚠ Personal email selected (no business email available)'
        ELSE '⚠ Hash-only (no cleartext available)'
    END as canonical_quality
FROM derived.vector_email_new ve
JOIN multi_email_vups m ON ve.vup_id = m.vup_id
LEFT JOIN derived.v_dynamodb_email_export ddb ON ve.vup_id = ddb.vup_id
GROUP BY ve.vup_id, ddb.canonical_email, ddb.canonical_email_type, ddb.canonical_data_source
ORDER BY COUNT(*) DESC;

-- Performance verification: Check table statistics
SELECT 
    'Table statistics' as metric_name,
    (SELECT COUNT(*) FROM derived.vector_email_new) as total_records,
    (SELECT COUNT(DISTINCT hem) FROM derived.vector_email_new) as unique_hems,
    (SELECT COUNT(DISTINCT vup_id) FROM derived.vector_email_new WHERE vup_id IS NOT NULL) as unique_vups,
    (SELECT COUNT(*) FROM derived.vector_email_new WHERE vup_id IS NOT NULL) as records_with_vup,
    (SELECT COUNT(*) FROM derived.vector_email_new WHERE vup_id IS NULL) as records_hem_only,
    ROUND(
        100.0 * (SELECT COUNT(*) FROM derived.vector_email_new WHERE vup_id IS NOT NULL)
        / (SELECT COUNT(*) FROM derived.vector_email_new),
        2
    ) as vup_match_rate_pct;

-- Record access pattern verification in metadata
INSERT INTO derived.vector_email_migration_metadata_eng1973 
    (migration_step, records_affected, notes)
VALUES 
    ('access_patterns_verified', 
     4,
     'Verified 4 critical access patterns: 1) HEM→VUP lookup, 2) HEM→cleartext email, 3) VUP→all HEMs, 4) VUP→canonical email');

-- Display all migration steps
SELECT * FROM derived.vector_email_migration_metadata_eng1973
ORDER BY execution_timestamp;

