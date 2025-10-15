-- ============================================================================
-- ENG-1914 Part 3: Identify duplicate groups
-- Creates temp tables with duplicate analysis
-- ============================================================================
-- NOTE: HEMs are explicitly EXCLUDED from duplicate detection.
-- Only LinkedIn URLs, FBF IDs, and PDL IDs are used to identify duplicates.
-- This prevents merging persons who share email addresses but are distinct entities.
-- ============================================================================

-- Create mapping of all identifiers to vup_ids
CREATE TEMP TABLE identifier_mapping AS
SELECT vup_id, fbf_id as identifier, 'fbf_id' as identifier_type
FROM vector_universal_person 
WHERE fbf_id IS NOT NULL AND TRIM(fbf_id) != ''
UNION ALL
SELECT vup_id, linkedin_url, 'linkedin_url'
FROM vector_universal_person 
WHERE linkedin_url IS NOT NULL AND TRIM(linkedin_url) != ''
UNION ALL
SELECT vup_id, pdl_id, 'pdl_id'
FROM vector_universal_person 
WHERE pdl_id IS NOT NULL AND TRIM(pdl_id) != '';

-- Find identifiers mapping to multiple vup_ids (duplicates)
CREATE TEMP TABLE duplicate_identifiers AS
SELECT 
  identifier,
  identifier_type,
  COUNT(DISTINCT vup_id) as vup_count,
  LISTAGG(DISTINCT vup_id, ',') WITHIN GROUP (ORDER BY vup_id) as vup_ids
FROM identifier_mapping
GROUP BY identifier, identifier_type
HAVING COUNT(DISTINCT vup_id) > 1;

-- Log statistics
SELECT 
  identifier_type,
  COUNT(*) as duplicate_identifier_count,
  SUM(vup_count) as total_vup_ids_affected
FROM duplicate_identifiers
GROUP BY identifier_type
ORDER BY duplicate_identifier_count DESC;

-- Create duplicate groups (connected components)
CREATE TEMP TABLE vup_duplicate_groups AS
WITH duplicate_connections AS (
  SELECT DISTINCT
    CAST(SPLIT_PART(vup_ids, ',', 1) AS BIGINT) as vup_id_1,
    CAST(SPLIT_PART(vup_ids, ',', ns.n) AS BIGINT) as vup_id_2
  FROM duplicate_identifiers
  CROSS JOIN (
    SELECT 2 as n UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL 
    SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL 
    SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
  ) ns
  WHERE SPLIT_PART(vup_ids, ',', ns.n) != ''
    AND SPLIT_PART(vup_ids, ',', 1) != SPLIT_PART(vup_ids, ',', ns.n)
),
connected_groups AS (
  SELECT 
    vup_id_1 as vup_id,
    MIN(LEAST(vup_id_1, vup_id_2)) OVER (PARTITION BY vup_id_1) as group_id
  FROM duplicate_connections
  UNION
  SELECT 
    vup_id_2 as vup_id,
    MIN(LEAST(vup_id_1, vup_id_2)) OVER (PARTITION BY vup_id_2) as group_id
  FROM duplicate_connections
)
SELECT 
  group_id,
  vup_id,
  ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY vup_id) as rank_in_group
FROM connected_groups;

-- Summary
SELECT 
  'Duplicate Groups Summary' as summary_type,
  COUNT(DISTINCT group_id) as total_groups,
  COUNT(*) as total_duplicate_vup_ids,
  COUNT(*) - COUNT(DISTINCT group_id) as vup_ids_to_merge
FROM vup_duplicate_groups;
