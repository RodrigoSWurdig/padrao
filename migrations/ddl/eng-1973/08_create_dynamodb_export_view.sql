-- ============================================================================
-- ENG-1973 Part 8: Create DynamoDB Export View
-- Purpose: Create view optimized for DynamoDB export with canonical email selection

-- ============================================================================

-- Drop existing DynamoDB export view if exists
DROP VIEW IF EXISTS derived.v_dynamodb_email_export CASCADE;

-- Create DynamoDB export view with canonical email logic
CREATE VIEW derived.v_dynamodb_email_export AS
WITH ranked_emails AS (
    SELECT 
        vup_id,
        hem,
        email,
        domain,
        email_type,
        data_source,
        last_verified,
        -- Rank emails per VUP for canonical selection
        ROW_NUMBER() OVER (
            PARTITION BY vup_id
            ORDER BY 
                -- Priority 1: Business emails first
                CASE WHEN email_type = 'business' THEN 1 ELSE 2 END,
                
                -- Priority 2: Cleartext over hash-only
                CASE WHEN email IS NOT NULL THEN 1 ELSE 2 END,
                
                -- Priority 3: Verified emails (future use)
                CASE WHEN last_verified IS NOT NULL THEN 1 ELSE 2 END,
                
                -- Priority 4: Most recent verification
                last_verified DESC NULLS LAST,
                
                -- Priority 5: Data source preference (5x5 > PDL)
                CASE WHEN data_source = '5x5' THEN 1 ELSE 2 END,
                
                -- Priority 6: Stable tiebreaker
                hem ASC
        ) as canonical_rank
    FROM derived.vector_email_new
    WHERE vup_id IS NOT NULL
)
SELECT 
    vup_id,
    hem as canonical_hem,
    email as canonical_email,
    domain as canonical_domain,
    email_type as canonical_email_type,
    data_source as canonical_data_source,
    last_verified as canonical_last_verified
FROM ranked_emails
WHERE canonical_rank = 1;

-- Grant permissions to view (adjust roles as needed for your environment)
-- GRANT SELECT ON derived.v_dynamodb_email_export TO <your_read_role>;
-- GRANT SELECT ON derived.v_dynamodb_email_export TO <dynamodb_export_user>;

-- Verify view structure
SELECT 
    column_name,
    data_type,
    character_maximum_length
FROM information_schema.columns
WHERE table_schema = 'derived'
  AND table_name = 'v_dynamodb_email_export'
ORDER BY ordinal_position;

-- Test view with sample query
SELECT 
    'DynamoDB export view test' as test_name,
    COUNT(*) as total_vups,
    COUNT(canonical_email) as vups_with_cleartext,
    COUNT(CASE WHEN canonical_email IS NULL THEN 1 END) as vups_hash_only,
    COUNT(CASE WHEN canonical_email_type = 'business' THEN 1 END) as vups_with_business_email,
    COUNT(CASE WHEN canonical_email_type = 'personal' THEN 1 END) as vups_with_personal_email,
    ROUND(100.0 * COUNT(canonical_email) / COUNT(*), 2) as pct_with_cleartext
FROM derived.v_dynamodb_email_export;

-- Sample canonical email selections for review
SELECT 
    vup_id,
    canonical_hem,
    canonical_email,
    canonical_domain,
    canonical_email_type,
    canonical_data_source
FROM derived.v_dynamodb_email_export
LIMIT 100;

-- Verify canonical selection logic: Compare VUPs with multiple emails
WITH multi_email_vups AS (
    SELECT vup_id
    FROM derived.vector_email_new
    WHERE vup_id IS NOT NULL
    GROUP BY vup_id
    HAVING COUNT(*) > 1
    LIMIT 10
)
SELECT 
    ve.vup_id,
    ve.hem,
    ve.email,
    ve.email_type,
    ve.data_source,
    CASE 
        WHEN ddb.canonical_hem = ve.hem THEN '✓ SELECTED AS CANONICAL'
        ELSE 'Not selected'
    END as canonical_status
FROM derived.vector_email_new ve
JOIN multi_email_vups m ON ve.vup_id = m.vup_id
LEFT JOIN derived.v_dynamodb_email_export ddb ON ve.vup_id = ddb.vup_id
ORDER BY ve.vup_id, 
         CASE WHEN ve.email_type = 'business' THEN 1 ELSE 2 END,
         CASE WHEN ve.email IS NOT NULL THEN 1 ELSE 2 END;

-- Record DynamoDB view creation in metadata
INSERT INTO derived.vector_email_migration_metadata_eng1973 
    (migration_step, records_affected, notes)
VALUES 
    ('dynamodb_export_view_created', 
     (SELECT COUNT(*) FROM derived.v_dynamodb_email_export),
     'Created v_dynamodb_email_export view with canonical email selection: business > cleartext > verified > 5x5 > PDL');

-- Display migration progress
SELECT * FROM derived.vector_email_migration_metadata_eng1973
ORDER BY execution_timestamp;

