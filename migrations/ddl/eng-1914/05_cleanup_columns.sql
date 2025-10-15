-- ============================================================================
-- ENG-1914 Part 5: Remove old columns (requires table recreation)
-- ============================================================================
-- ⚠️ CRITICAL SEQUENCING REQUIREMENT ⚠️
--
-- DO NOT EXECUTE until cube schema changes deploy to production AND
-- you verify no active queries reference linkedin_url, fbf_id, pdl_id.
--
-- Run this validation query first:
--   SELECT query, starttime, querytxt FROM stl_query
--   WHERE querytxt ILIKE '%vector_universal_person%'
--     AND (querytxt ILIKE '%linkedin_url%' OR querytxt ILIKE '%fbf_id%' OR querytxt ILIKE '%pdl_id%')
--     AND starttime > DATEADD(hour, -24, GETDATE())
--   LIMIT 20;
--
-- Recommended wait: 2 weeks after cube deployment before executing.
-- ============================================================================

-- Create new table without old identifier columns
CREATE TABLE vector_universal_person_new (
  vup_id BIGINT NOT NULL PRIMARY KEY,
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  full_name VARCHAR(255),
  title VARCHAR(255),
  company VARCHAR(255),
  location VARCHAR(255),
  country VARCHAR(100),
  state VARCHAR(100),
  city VARCHAR(100),
  -- linkedin_url, fbf_id, pdl_id removed (now in separate tables)
  merged_into_vup_id BIGINT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
) DISTSTYLE KEY DISTKEY(vup_id) SORTKEY(vup_id);

-- Copy data (excluding removed columns)
INSERT INTO vector_universal_person_new
SELECT 
  vup_id, first_name, last_name, full_name, title,
  company, location, country, state, city,
  merged_into_vup_id, created_at, updated_at
FROM vector_universal_person
WHERE merged_into_vup_id IS NULL;  -- Only keep active records

-- Atomic table swap
BEGIN TRANSACTION;
  DROP TABLE vector_universal_person;
  ALTER TABLE vector_universal_person_new RENAME TO vector_universal_person;
  GRANT SELECT ON vector_universal_person TO analytics_role;
COMMIT;
