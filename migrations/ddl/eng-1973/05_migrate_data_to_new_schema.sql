-- ============================================================================
-- ENG-1973 Part 5: Migrate Data to New Schema with Deduplication
-- Purpose: Migrate data from old schema to new with priority-based deduplication
-- Strategy: Two-phase insert (hem+vup pairs first, then hem-only records)
-- Execution Time: 20-30 minutes (depends on dataset size)
-- ============================================================================

-- Phase 1: Migrate records with both hem and vup_id (enforcing uniqueness)
-- Priority: cleartext > hash-only, business email > personal email, 5x5 > PDL
INSERT INTO derived.vector_email_new (
    hem,
    email,
    vup_id,
    domain,
    email_type,
    data_source,
    dataset_version,
    last_verified,
    created_at,
    updated_at
)
SELECT 
    sha256 as hem,
    email,
    vup_id,
    CASE 
        WHEN email IS NOT NULL THEN LOWER(SPLIT_PART(email, '@', 2))
        ELSE NULL
    END as domain,
    CASE 
        WHEN COALESCE(d.type, 'company') = 'personal' THEN 'personal'
        WHEN COALESCE(d.type, 'company') = 'spam' THEN 'personal'  -- Treat spam as personal
        ELSE 'business'
    END as email_type,
    data_source,
    dataset_version,
    NULL as last_verified,                   -- Future use for deliverability tracking
    GETDATE() as created_at,
    GETDATE() as updated_at
FROM (
    SELECT 
        sha256,
        email,
        vup_id,
        email_type,
        data_source,
        dataset_version,
        ROW_NUMBER() OVER (
            PARTITION BY sha256, vup_id 
            ORDER BY 
                -- Priority 1: Prefer cleartext over hash-only
                CASE WHEN email IS NOT NULL THEN 1 ELSE 2 END,
                
                -- Priority 2: Prefer business over personal
                CASE WHEN email_type = 'business' THEN 1 ELSE 2 END,
                
                -- Priority 3: Prefer 5x5 over PDL (5x5 has better email quality)
                CASE WHEN data_source = '5x5' THEN 1 ELSE 2 END,
                
                -- Priority 4: Most recent dataset version
                dataset_version DESC,
                
                -- Priority 5: Stable tiebreaker
                email DESC
        ) as priority_rank
    FROM derived.vector_email
    WHERE vup_id IS NOT NULL
) ranked
LEFT JOIN derived.vector_email_domains d 
    ON LOWER(SPLIT_PART(ranked.email, '@', 2)) = d.domain
WHERE priority_rank = 1;

-- Record Phase 1 completion
INSERT INTO derived.vector_email_migration_metadata_eng1973 
    (migration_step, records_affected, notes)
VALUES 
    ('phase1_hem_vup_pairs', 
     (SELECT COUNT(*) FROM derived.vector_email_new),
     'Migrated (hem, vup_id) pairs with priority-based deduplication: cleartext > hash-only, business > personal, 5x5 > PDL');

-- Phase 2: Migrate hem-only records (no vup_id)
-- These are HEMs that couldn't be matched to a VUP yet
INSERT INTO derived.vector_email_new (
    hem,
    email,
    vup_id,
    domain,
    email_type,
    data_source,
    dataset_version,
    last_verified,
    created_at,
    updated_at
)
SELECT 
    sha256 as hem,
    email,
    NULL as vup_id,
    CASE 
        WHEN email IS NOT NULL THEN LOWER(SPLIT_PART(email, '@', 2))
        ELSE NULL
    END as domain,
    CASE 
        WHEN COALESCE(d.type, 'company') = 'personal' THEN 'personal'
        WHEN COALESCE(d.type, 'company') = 'spam' THEN 'personal'
        ELSE 'business'
    END as email_type,
    data_source,
    dataset_version,
    NULL as last_verified,
    GETDATE() as created_at,
    GETDATE() as updated_at
FROM (
    SELECT 
        sha256,
        email,
        email_type,
        data_source,
        dataset_version,
        ROW_NUMBER() OVER (
            PARTITION BY sha256
            ORDER BY 
                -- Same priority logic as Phase 1
                CASE WHEN email IS NOT NULL THEN 1 ELSE 2 END,
                CASE WHEN email_type = 'business' THEN 1 ELSE 2 END,
                CASE WHEN data_source = '5x5' THEN 1 ELSE 2 END,
                dataset_version DESC,
                email DESC
        ) as priority_rank
    FROM derived.vector_email
    WHERE vup_id IS NULL
) ranked
LEFT JOIN derived.vector_email_domains d 
    ON LOWER(SPLIT_PART(ranked.email, '@', 2)) = d.domain
WHERE priority_rank = 1;

-- Record Phase 2 completion
INSERT INTO derived.vector_email_migration_metadata_eng1973 
    (migration_step, records_affected, notes)
VALUES 
    ('phase2_hem_only', 
     (SELECT COUNT(*) FROM derived.vector_email_new WHERE vup_id IS NULL),
     'Migrated hem-only records (unmatched to VUP) with same priority-based deduplication');

-- Verify migration completeness
SELECT 
    'Migration completeness check' as check_name,
    (SELECT COUNT(*) FROM derived.vector_email) as original_total,
    (SELECT COUNT(*) FROM derived.vector_email_new) as migrated_total,
    (SELECT COUNT(*) FROM derived.vector_email_new WHERE vup_id IS NOT NULL) as with_vup_id,
    (SELECT COUNT(*) FROM derived.vector_email_new WHERE vup_id IS NULL) as hem_only,
    (SELECT COUNT(*) FROM derived.vector_email) - (SELECT COUNT(*) FROM derived.vector_email_new) as records_deduplicated,
    ROUND(
        100.0 * ((SELECT COUNT(*) FROM derived.vector_email) - (SELECT COUNT(*) FROM derived.vector_email_new)) 
        / (SELECT COUNT(*) FROM derived.vector_email),
        2
    ) as deduplication_pct;

-- Verify (hem, vup_id) uniqueness was enforced
SELECT 
    'Uniqueness constraint verification' as check_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT (hem, vup_id)) as unique_hem_vup_pairs,
    CASE 
        WHEN COUNT(*) = COUNT(DISTINCT (hem, vup_id)) 
        THEN 'PASS - All (hem, vup_id) pairs are unique'
        ELSE 'FAIL - Duplicate (hem, vup_id) pairs exist'
    END as status
FROM derived.vector_email_new
WHERE vup_id IS NOT NULL;

-- Verify cleartext email preservation
SELECT 
    'Cleartext email preservation' as check_name,
    (SELECT COUNT(*) FROM derived.vector_email WHERE email IS NOT NULL) as original_cleartext,
    (SELECT COUNT(*) FROM derived.vector_email_new WHERE email IS NOT NULL) as migrated_cleartext,
    CASE 
        WHEN (SELECT COUNT(*) FROM derived.vector_email_new WHERE email IS NOT NULL) >= 
             (SELECT COUNT(DISTINCT sha256) FROM derived.vector_email WHERE email IS NOT NULL)
        THEN 'PASS - At least one cleartext email preserved per unique HEM'
        ELSE 'WARNING - Some cleartext emails may have been lost'
    END as status;

-- Verify email type distribution
SELECT 
    'Email type distribution' as metric,
    email_type,
    COUNT(*) as record_count,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM derived.vector_email_new), 2) as percentage
FROM derived.vector_email_new
GROUP BY email_type
ORDER BY record_count DESC;

-- Verify data source distribution
SELECT 
    'Data source distribution' as metric,
    data_source,
    COUNT(*) as record_count,
    COUNT(CASE WHEN email IS NOT NULL THEN 1 END) as with_cleartext,
    ROUND(100.0 * COUNT(CASE WHEN email IS NOT NULL THEN 1 END) / COUNT(*), 2) as pct_cleartext
FROM derived.vector_email_new
GROUP BY data_source
ORDER BY record_count DESC;

-- Sample migrated records for manual review
SELECT 
    hem,
    email,
    vup_id,
    domain,
    email_type,
    data_source,
    dataset_version
FROM derived.vector_email_new
LIMIT 100;

-- Expected verification results:
-- ✓ Migration completeness: migrated_total should be < original_total (due to deduplication)
-- ✓ Uniqueness constraint: PASS status (all (hem, vup_id) pairs unique)
-- ✓ Cleartext preservation: PASS status (no cleartext emails lost for unique HEMs)
-- ✓ Email type distribution: Should show business vs personal breakdown
-- ✓ Data source distribution: Should show 5x5 vs PDL breakdown with cleartext percentages
