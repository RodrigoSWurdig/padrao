-- ============================================================================
-- ENG-1914 Part 4: Select winner for each group and merge duplicates
-- Winner = most jobs + most recent update
-- ============================================================================

-- Determine winner for each duplicate group
CREATE TEMP TABLE duplicate_winners AS
SELECT 
  dg.group_id,
  dg.vup_id,
  vup.created_at,
  vup.updated_at,
  COALESCE(job_counts.job_count, 0) as job_count,
  ROW_NUMBER() OVER (
    PARTITION BY dg.group_id 
    ORDER BY 
      COALESCE(job_counts.job_count, 0) DESC,  -- Most jobs wins
      vup.updated_at DESC,                       -- Most recent wins
      vup.created_at ASC                        -- TEMPORAL TIEBREAKER: Oldest person record wins
  ) as winner_rank
FROM vup_duplicate_groups dg
INNER JOIN vector_universal_person vup ON dg.vup_id = vup.vup_id
LEFT JOIN (
  SELECT vup_id, COUNT(*) as job_count
  FROM vector_universal_job
  GROUP BY vup_id
) job_counts ON vup.vup_id = job_counts.vup_id;

-- Create merge mapping (losers to winners)
CREATE TEMP TABLE merge_mapping AS
SELECT 
  losers.vup_id as loser_id,
  winners.vup_id as winner_id,
  losers.group_id
FROM duplicate_winners losers
INNER JOIN (
  SELECT group_id, vup_id 
  FROM duplicate_winners 
  WHERE winner_rank = 1
) winners ON losers.group_id = winners.group_id
WHERE losers.winner_rank > 1;

-- Log merge plan
SELECT 
  'Merge Plan' as summary,
  COUNT(DISTINCT group_id) as groups_to_merge,
  COUNT(*) as loser_records,
  COUNT(DISTINCT winner_id) as winner_records
FROM merge_mapping;

-- BEGIN MERGE TRANSACTION
BEGIN TRANSACTION;

-- Update all child tables to point to winners
UPDATE vector_universal_job
SET vup_id = mm.winner_id
FROM merge_mapping mm
WHERE vector_universal_job.vup_id = mm.loser_id;

UPDATE vector_emails
SET vup_id = mm.winner_id
FROM merge_mapping mm
WHERE vector_emails.vup_id = mm.loser_id;

-- Migrate identifiers from losers to winners
INSERT INTO vup_linkedin_urls (vup_id, linkedin_url, created_at)
SELECT DISTINCT mm.winner_id, lu.linkedin_url, GETDATE()
FROM merge_mapping mm
INNER JOIN vup_linkedin_urls lu ON mm.loser_id = lu.vup_id
WHERE NOT EXISTS (
  SELECT 1 FROM vup_linkedin_urls existing
  WHERE existing.vup_id = mm.winner_id 
    AND existing.linkedin_url = lu.linkedin_url
);

INSERT INTO vup_fbf_ids (vup_id, fbf_id, created_at)
SELECT DISTINCT mm.winner_id, fbf.fbf_id, GETDATE()
FROM merge_mapping mm
INNER JOIN vup_fbf_ids fbf ON mm.loser_id = fbf.vup_id
WHERE NOT EXISTS (
  SELECT 1 FROM vup_fbf_ids existing
  WHERE existing.vup_id = mm.winner_id 
    AND existing.fbf_id = fbf.fbf_id
);

INSERT INTO vup_pdl_ids (vup_id, pdl_id, created_at)
SELECT DISTINCT mm.winner_id, pdl.pdl_id, GETDATE()
FROM merge_mapping mm
INNER JOIN vup_pdl_ids pdl ON mm.loser_id = pdl.vup_id
WHERE NOT EXISTS (
  SELECT 1 FROM vup_pdl_ids existing
  WHERE existing.vup_id = mm.winner_id 
    AND existing.pdl_id = pdl.pdl_id
);

-- Delete loser identifiers
DELETE FROM vup_linkedin_urls WHERE vup_id IN (SELECT loser_id FROM merge_mapping);
DELETE FROM vup_fbf_ids WHERE vup_id IN (SELECT loser_id FROM merge_mapping);
DELETE FROM vup_pdl_ids WHERE vup_id IN (SELECT loser_id FROM merge_mapping);

-- Mark losers as merged (soft delete)
ALTER TABLE vector_universal_person ADD COLUMN merged_into_vup_id BIGINT;

UPDATE vector_universal_person
SET merged_into_vup_id = mm.winner_id
FROM merge_mapping mm
WHERE vector_universal_person.vup_id = mm.loser_id;

COMMIT;

-- Verification
SELECT 
  COUNT(*) as total_merged_records,
  COUNT(DISTINCT merged_into_vup_id) as unique_winners
FROM vector_universal_person
WHERE merged_into_vup_id IS NOT NULL;
