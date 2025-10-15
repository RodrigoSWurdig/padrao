-- ============================================================================
-- ENG-1914 Part 1: Create backup and new identifier tables
-- ============================================================================

-- Create full backup
CREATE TABLE vector_universal_person_backup AS 
SELECT * FROM vector_universal_person;

GRANT SELECT ON vector_universal_person_backup TO analytics_role;

-- Create new table for LinkedIn URLs (one-to-many with vup_id)
CREATE TABLE vup_linkedin_urls (
  vup_id BIGINT NOT NULL,
  linkedin_url VARCHAR(500) NOT NULL,
  created_at TIMESTAMP DEFAULT GETDATE(),
  PRIMARY KEY (vup_id, linkedin_url)
) DISTSTYLE KEY 
  DISTKEY(vup_id) 
  SORTKEY(vup_id, linkedin_url);

-- Create new table for FBF IDs
CREATE TABLE vup_fbf_ids (
  vup_id BIGINT NOT NULL,
  fbf_id VARCHAR(100) NOT NULL,
  created_at TIMESTAMP DEFAULT GETDATE(),
  PRIMARY KEY (vup_id, fbf_id)
) DISTSTYLE KEY 
  DISTKEY(vup_id) 
  SORTKEY(vup_id, fbf_id);

-- Create new table for PDL IDs
CREATE TABLE vup_pdl_ids (
  vup_id BIGINT NOT NULL,
  pdl_id VARCHAR(100) NOT NULL,
  created_at TIMESTAMP DEFAULT GETDATE(),
  PRIMARY KEY (vup_id, pdl_id)
) DISTSTYLE KEY 
  DISTKEY(vup_id) 
  SORTKEY(vup_id, pdl_id);

-- Grant permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON vup_linkedin_urls TO analytics_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON vup_fbf_ids TO analytics_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON vup_pdl_ids TO analytics_role;
