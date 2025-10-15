-- ============================================================================
-- ENG-1973 Part 7: Update Dependent Views
-- Purpose: Update temp_vector_emails view to use new schema
-- ============================================================================

-- Backup existing view definition
-- (Manual step: Save current view DDL before dropping)

-- Drop existing temp_vector_emails view
DROP VIEW IF EXISTS derived.temp_vector_emails CASCADE;

-- Recreate temp_vector_emails view with new schema
CREATE VIEW derived.temp_vector_emails AS
SELECT 
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
FROM derived.vector_email_new;

-- Grant permissions to view (adjust roles as needed for your environment)
-- GRANT SELECT ON derived.temp_vector_emails TO <your_read_role>;

-- Verify view was created successfully
SELECT 
    schemaname,
    viewname,
    definition
FROM pg_views
WHERE schemaname = 'derived'
  AND viewname = 'temp_vector_emails';

-- Test view with sample query
SELECT 
    'View test query' as test_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT hem) as unique_hems,
    COUNT(DISTINCT vup_id) as unique_vups,
    COUNT(CASE WHEN email IS NOT NULL THEN 1 END) as with_cleartext,
    COUNT(CASE WHEN email IS NULL THEN 1 END) as hash_only
FROM derived.temp_vector_emails;

-- Record view update in metadata
INSERT INTO derived.vector_email_migration_metadata_eng1973 
    (migration_step, records_affected, notes)
VALUES 
    ('view_update_completed', 
     (SELECT COUNT(*) FROM derived.temp_vector_emails),
     'Updated temp_vector_emails view to use new vector_email_new schema');

-- Verify dependent objects still work
-- (Manual step: Test any applications/queries that use temp_vector_emails)

-- Display migration progress
SELECT * FROM derived.vector_email_migration_metadata_eng1973
ORDER BY execution_timestamp;

