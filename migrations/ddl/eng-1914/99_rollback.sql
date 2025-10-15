-- ============================================================================
-- ENG-1914 ROLLBACK SCRIPT - Use only if issues occur
-- ============================================================================

BEGIN TRANSACTION;
  -- Restore from backup
  DROP TABLE IF EXISTS vector_universal_person;
  CREATE TABLE vector_universal_person AS 
  SELECT * FROM vector_universal_person_backup;
  
  -- Drop new tables
  DROP TABLE IF EXISTS vup_linkedin_urls;
  DROP TABLE IF EXISTS vup_fbf_ids;
  DROP TABLE IF EXISTS vup_pdl_ids;
COMMIT;
