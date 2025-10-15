-- ============================================================================
-- ENG-1973 Part 3: Create New Schema with UNIQUE Constraint
-- ============================================================================

-- Drop new table if exists (for idempotency during testing)
DROP TABLE IF EXISTS derived.vector_email_new CASCADE;

-- Create new vector_email table with enforced uniqueness
CREATE TABLE derived.vector_email_new (
    -- Core identifiers
    hem VARCHAR(256) NOT NULL,              -- Hashed email (sha256) - required
    email VARCHAR(1024),                     -- Cleartext email (nullable for hash-only records)
    vup_id VARCHAR(32),                      -- Vector Universal Person ID (nullable for unmatched HEMs)
    
    -- Classification fields
    domain VARCHAR(255),                     -- Email domain for type classification
    email_type VARCHAR(20),                  -- 'business' or 'personal'
    
    -- Data lineage fields
    data_source VARCHAR(50) NOT NULL,        -- Source system ('5x5', 'PDL', etc.)
    dataset_version VARCHAR(20),             -- Version of source dataset
    
    -- Quality tracking fields
    last_verified TIMESTAMP,                 -- Last deliverability verification (future use)
    
    -- Metadata
    created_at TIMESTAMP DEFAULT GETDATE(),
    updated_at TIMESTAMP DEFAULT GETDATE(),
    
    -- Enforce single source of truth for (hem, vup_id) combination
    UNIQUE(hem, vup_id)
)
DISTSTYLE KEY
DISTKEY(hem)                                 -- Optimize for hem-based lookups
SORTKEY(vup_id, hem);                        -- Optimize for person-based queries

-- Create indexes for common access patterns
-- Note: Redshift doesn't support explicit indexes, but SORTKEY handles this

-- Record schema creation in metadata
INSERT INTO derived.vector_email_migration_metadata_eng1973 
    (migration_step, records_affected, notes)
VALUES 
    ('new_schema_created', 
     0,
     'Created vector_email_new table with UNIQUE(hem, vup_id) constraint, DISTKEY(hem), SORTKEY(vup_id, hem)');

-- Verify table structure
SELECT 
    column_name,
    data_type,
    character_maximum_length,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_schema = 'derived'
  AND table_name = 'vector_email_new'
ORDER BY ordinal_position;

-- Verify distribution and sort keys
SELECT 
    tablename,
    diststyle,
    distkey,
    sortkey1,
    sortkey2
FROM pg_table_def
WHERE schemaname = 'derived'
  AND tablename = 'vector_email_new';

