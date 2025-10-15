-- ============================================================================
-- ENG-1973 Part 2: Create Backup Tables
-- Purpose: Create backup of current vector_email table and migration metadata tracking

-- ============================================================================

-- Create backup of current vector_email table
DROP TABLE IF EXISTS derived.vector_email_backup_eng1973 CASCADE;

CREATE TABLE derived.vector_email_backup_eng1973
DISTSTYLE KEY
DISTKEY(sha256)
SORTKEY(vup_id, sha256)
AS
SELECT 
    *,
    GETDATE() as backup_timestamp
FROM derived.vector_email;

-- Verify backup record count
SELECT 
    'Backup verification' as check_name,
    (SELECT COUNT(*) FROM derived.vector_email) as original_count,
    (SELECT COUNT(*) FROM derived.vector_email_backup_eng1973) as backup_count,
    CASE 
        WHEN (SELECT COUNT(*) FROM derived.vector_email) = (SELECT COUNT(*) FROM derived.vector_email_backup_eng1973)
        THEN 'PASS'
        ELSE 'FAIL'
    END as status;

-- Create migration metadata tracking table
DROP TABLE IF EXISTS derived.vector_email_migration_metadata_eng1973 CASCADE;

CREATE TABLE derived.vector_email_migration_metadata_eng1973 (
    migration_step VARCHAR(100),
    execution_timestamp TIMESTAMP DEFAULT GETDATE(),
    records_affected BIGINT,
    notes VARCHAR(5000)
)
DISTSTYLE ALL;

-- Record backup completion
INSERT INTO derived.vector_email_migration_metadata_eng1973 
    (migration_step, records_affected, notes)
VALUES 
    ('backup_created', 
     (SELECT COUNT(*) FROM derived.vector_email_backup_eng1973),
     'Full backup of vector_email table created as vector_email_backup_eng1973');

-- Display migration metadata
SELECT * FROM derived.vector_email_migration_metadata_eng1973
ORDER BY execution_timestamp;
