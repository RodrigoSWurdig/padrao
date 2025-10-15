-- ============================================================================
-- ENG-1914 Part 2: Migrate identifier data to new tables
-- ============================================================================

-- Migrate LinkedIn URLs
INSERT INTO vup_linkedin_urls (vup_id, linkedin_url)
SELECT vup_id, TRIM(linkedin_url) as linkedin_url
FROM vector_universal_person 
WHERE linkedin_url IS NOT NULL AND TRIM(linkedin_url) != '';

-- Migrate FBF IDs
INSERT INTO vup_fbf_ids (vup_id, fbf_id)
SELECT vup_id, TRIM(fbf_id) as fbf_id
FROM vector_universal_person 
WHERE fbf_id IS NOT NULL AND TRIM(fbf_id) != '';

-- Migrate PDL IDs
INSERT INTO vup_pdl_ids (vup_id, pdl_id)
SELECT vup_id, TRIM(pdl_id) as pdl_id
FROM vector_universal_person 
WHERE pdl_id IS NOT NULL AND TRIM(pdl_id) != '';

-- Verification
SELECT 
  'Migration Verification' as check_type,
  COUNT(CASE WHEN linkedin_url IS NOT NULL THEN 1 END) as linkedin_in_original,
  (SELECT COUNT(*) FROM vup_linkedin_urls) as linkedin_migrated,
  COUNT(CASE WHEN fbf_id IS NOT NULL THEN 1 END) as fbf_in_original,
  (SELECT COUNT(*) FROM vup_fbf_ids) as fbf_migrated,
  COUNT(CASE WHEN pdl_id IS NOT NULL THEN 1 END) as pdl_in_original,
  (SELECT COUNT(*) FROM vup_pdl_ids) as pdl_migrated
FROM vector_universal_person;
