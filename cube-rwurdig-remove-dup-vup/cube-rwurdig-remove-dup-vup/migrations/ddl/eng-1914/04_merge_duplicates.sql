-- ============================================================================
-- ENG-1914 Part 4: Merge Duplicate Person Records with Winner Selection
-- Purpose: Consolidate duplicate persons while preserving complete data
-- Operations: DESTRUCTIVE - Modifies data by merging losers into winners
-- Winner Selection: Most jobs > Most HEMs > Most recent update > Lowest ID
-- HEM Handling: Ensures (vup_id, hem) uniqueness for ENG-1973 preparation
-- Prerequisites: Part 3 completed, duplicate groups analyzed and documented
-- CRITICAL: Execute within TRANSACTION with comprehensive verification
-- ============================================================================

-- Prerequisite verification
SELECT 
    'Pre-Merge Verification' as checkpoint_type,
    (SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'pg_temp' AND tablename LIKE '%vup_duplicate_groups%') as duplicate_groups_table_exists,
    (SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'pg_temp' AND tablename LIKE '%duplicate_edges%') as duplicate_edges_table_exists,
    CASE 
        WHEN (SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'pg_temp' AND tablename LIKE '%vup_duplicate_groups%') > 0
         AND (SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'pg_temp' AND tablename LIKE '%duplicate_edges%') > 0
        THEN 'READY - Part 3 temporary tables present'
        ELSE 'ERROR - Part 3 must be executed first to create duplicate groups'
    END as readiness_status;

BEGIN TRANSACTION;

-- ============================================================================
-- STEP 1: CALCULATE COMPREHENSIVE METRICS FOR WINNER SELECTION
-- ============================================================================

DROP TABLE IF EXISTS #person_metrics;

CREATE TEMP TABLE #person_metrics AS
SELECT 
    vup.vup_id,
    COALESCE(jc.job_count, 0) as job_count,
    COALESCE(jc.most_recent_job_update, vup.updated_at) as most_recent_job_activity,
    COALESCE(ec.email_count, 0) as email_count,
    COALESCE(ec.unique_hem_count, 0) as unique_hem_count,
    COALESCE(ec.business_email_count, 0) as business_email_count,
    vup.updated_at as person_updated_at,
    vup.created_at as person_created_at,
    CASE WHEN vup.first_name IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN vup.last_name IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN vup.street_address IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN vup.locality IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN vup.region IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN vup.country IS NOT NULL THEN 1 ELSE 0 END as person_field_completeness_score
FROM derived.vector_universal_person vup
LEFT JOIN (
    SELECT 
        vup_id, 
        COUNT(*) as job_count,
        MAX(updated_at) as most_recent_job_update
    FROM derived.vector_universal_job
    GROUP BY vup_id
) jc ON vup.vup_id = jc.vup_id
LEFT JOIN (
    SELECT 
        vup_id, 
        COUNT(*) as email_count,
        COUNT(DISTINCT sha256) as unique_hem_count,
        COUNT(CASE WHEN email_type = 'business' THEN 1 END) as business_email_count
    FROM derived.vector_email
    WHERE vup_id IS NOT NULL
    GROUP BY vup_id
) ec ON vup.vup_id = ec.vup_id;

-- Display metrics distribution for winner selection validation
SELECT 
    'Person Metrics for Winner Selection' as metrics_summary,
    COUNT(*) as total_persons_analyzed,
    ROUND(AVG(job_count), 2) as avg_jobs_per_person,
    ROUND(AVG(unique_hem_count), 2) as avg_hems_per_person,
    ROUND(AVG(person_field_completeness_score), 2) as avg_field_completeness,
    MAX(job_count) as max_jobs_on_single_person,
    MAX(unique_hem_count) as max_hems_on_single_person
FROM #person_metrics;

-- ============================================================================
-- STEP 2: DETERMINE WINNERS FOR EACH DUPLICATE GROUP
-- ============================================================================

-- Winner Selection Priority (highest priority first):
--   1. Most related job records (indicates richer professional data)
--   2. Most unique HEMs (indicates better email identity resolution)
--   3. Most recent activity (person_updated_at or job activity)
--   4. Highest person field completeness (more populated demographic fields)
--   5. Lowest vup_id (deterministic tiebreaker, not chronological due to UUID)

DROP TABLE IF EXISTS #merge_mapping;

CREATE TEMP TABLE #merge_mapping AS
WITH ranked_duplicates AS (
    SELECT 
        vdg.group_id,
        vdg.vup_id,
        pm.job_count,
        pm.unique_hem_count,
        pm.business_email_count,
        pm.email_count,
        pm.person_updated_at,
        pm.most_recent_job_activity,
        pm.person_field_completeness_score,
        ROW_NUMBER() OVER (
            PARTITION BY vdg.group_id 
            ORDER BY 
                pm.job_count DESC,                              -- Most jobs wins
                pm.unique_hem_count DESC,                       -- Most HEMs wins
                GREATEST(pm.person_updated_at, pm.most_recent_job_activity) DESC NULLS LAST,  -- Most recent wins
                pm.person_field_completeness_score DESC,        -- Most complete wins
                vdg.vup_id ASC                                  -- Lowest ID tiebreaker
        ) as rank_within_group
    FROM #vup_duplicate_groups vdg
    INNER JOIN #person_metrics pm ON vdg.vup_id = pm.vup_id
    INNER JOIN derived.vector_universal_person vup ON vdg.vup_id = vup.vup_id
    WHERE vup.merged_into_vup_id IS NULL  -- Exclude already merged records from prior runs
)
SELECT 
    losers.group_id,
    winners.vup_id as winner_id,
    losers.vup_id as loser_id,
    winners.job_count as winner_job_count,
    losers.job_count as loser_job_count,
    winners.unique_hem_count as winner_hem_count,
    losers.unique_hem_count as loser_hem_count,
    winners.email_count as winner_email_count,
    losers.email_count as loser_email_count,
    winners.business_email_count as winner_business_email_count,
    losers.business_email_count as loser_business_email_count,
    winners.person_field_completeness_score as winner_completeness,
    losers.person_field_completeness_score as loser_completeness
FROM ranked_duplicates winners
INNER JOIN ranked_duplicates losers 
    ON winners.group_id = losers.group_id
WHERE winners.rank_within_group = 1  -- Winner records
  AND losers.rank_within_group > 1;  -- Loser records

-- Display merge plan summary for validation
SELECT 
    'Merge Plan Summary' as summary_type,
    COUNT(DISTINCT group_id) as groups_to_merge,
    COUNT(DISTINCT winner_id) as unique_winners,
    COUNT(DISTINCT loser_id) as unique_losers_to_merge,
    COUNT(*) as total_merge_operations,
    SUM(winner_job_count) as total_winner_jobs,
    SUM(loser_job_count) as total_loser_jobs_to_migrate,
    SUM(winner_hem_count) as total_winner_hems,
    SUM(loser_hem_count) as total_loser_hems_to_migrate,
    ROUND(AVG(winner_job_count), 2) as avg_winner_jobs,
    ROUND(AVG(loser_job_count), 2) as avg_loser_jobs
FROM #merge_mapping;

-- Sample merge decisions for quality assurance review
SELECT 
    'Sample Merge Decisions for Review' as review_type,
    group_id,
    winner_id,
    loser_id,
    winner_job_count,
    loser_job_count,
    winner_hem_count,
    loser_hem_count,
    winner_completeness,
    loser_completeness,
    'Winner Profile: ' || CAST(winner_job_count AS VARCHAR) || ' jobs, ' || 
        CAST(winner_hem_count AS VARCHAR) || ' HEMs, completeness=' || 
        CAST(winner_completeness AS VARCHAR) as winner_profile,
    'Loser Profile: ' || CAST(loser_job_count AS VARCHAR) || ' jobs, ' || 
        CAST(loser_hem_count AS VARCHAR) || ' HEMs, completeness=' || 
        CAST(loser_completeness AS VARCHAR) as loser_profile
FROM #merge_mapping
ORDER BY ABS(winner_hem_count - loser_hem_count) DESC
LIMIT 20;

-- VERIFICATION CHECKPOINT: Review above before proceeding
SELECT 
    'CRITICAL CHECKPOINT: Review Required' as checkpoint_type,
    'Review merge plan summary and sample merge decisions above' as action_required,
    'Verify winner selection criteria producing appropriate results' as validation_step,
    'Uncomment remaining script sections ONLY after thorough review' as next_step;

-- ============================================================================
-- CRITICAL: Uncomment sections below ONLY after reviewing merge plan
-- ============================================================================

/*

-- ============================================================================
-- STEP 3: MIGRATE IDENTIFIER ASSOCIATIONS FROM LOSERS TO WINNERS
-- ============================================================================

-- LinkedIn URLs - preserve all unique URLs from both winners and losers
INSERT INTO derived.vup_linkedin_urls (vup_id, linkedin_url, created_at)
SELECT DISTINCT 
    mm.winner_id,
    lu.linkedin_url,
    GETDATE()
FROM #merge_mapping mm
INNER JOIN derived.vup_linkedin_urls lu ON mm.loser_id = lu.vup_id
WHERE NOT EXISTS (
    SELECT 1 FROM derived.vup_linkedin_urls existing
    WHERE existing.vup_id = mm.winner_id 
      AND existing.linkedin_url = lu.linkedin_url
);

-- FBF IDs - preserve all unique FBF IDs from both winners and losers
INSERT INTO derived.vup_fbf_ids (vup_id, fbf_id, created_at)
SELECT DISTINCT 
    mm.winner_id,
    fbf.fbf_id,
    GETDATE()
FROM #merge_mapping mm
INNER JOIN derived.vup_fbf_ids fbf ON mm.loser_id = fbf.vup_id
WHERE NOT EXISTS (
    SELECT 1 FROM derived.vup_fbf_ids existing
    WHERE existing.vup_id = mm.winner_id 
      AND existing.fbf_id = fbf.fbf_id
);

-- PDL IDs - preserve all unique PDL IDs from both winners and losers
INSERT INTO derived.vup_pdl_ids (vup_id, pdl_id, created_at)
SELECT DISTINCT 
    mm.winner_id,
    pdl.pdl_id,
    GETDATE()
FROM #merge_mapping mm
INNER JOIN derived.vup_pdl_ids pdl ON mm.loser_id = pdl.vup_id
WHERE NOT EXISTS (
    SELECT 1 FROM derived.vup_pdl_ids existing
    WHERE existing.vup_id = mm.winner_id 
      AND existing.pdl_id = pdl.pdl_id
);

-- Verify identifier migration before deletion
SELECT 
    'Identifier Migration Verification' as check_type,
    (SELECT COUNT(*) FROM derived.vup_linkedin_urls 
     WHERE vup_id IN (SELECT winner_id FROM #merge_mapping)) as winner_linkedin_urls,
    (SELECT COUNT(*) FROM derived.vup_linkedin_urls 
     WHERE vup_id IN (SELECT loser_id FROM #merge_mapping)) as loser_linkedin_urls_remaining,
    (SELECT COUNT(*) FROM derived.vup_fbf_ids 
     WHERE vup_id IN (SELECT winner_id FROM #merge_mapping)) as winner_fbf_ids,
    (SELECT COUNT(*) FROM derived.vup_fbf_ids 
     WHERE vup_id IN (SELECT loser_id FROM #merge_mapping)) as loser_fbf_ids_remaining,
    (SELECT COUNT(*) FROM derived.vup_pdl_ids 
     WHERE vup_id IN (SELECT winner_id FROM #merge_mapping)) as winner_pdl_ids,
    (SELECT COUNT(*) FROM derived.vup_pdl_ids 
     WHERE vup_id IN (SELECT loser_id FROM #merge_mapping)) as loser_pdl_ids_remaining;

-- Delete loser identifier associations after successful migration
DELETE FROM derived.vup_linkedin_urls 
WHERE vup_id IN (SELECT loser_id FROM #merge_mapping);

DELETE FROM derived.vup_fbf_ids 
WHERE vup_id IN (SELECT loser_id FROM #merge_mapping);

DELETE FROM derived.vup_pdl_ids 
WHERE vup_id IN (SELECT loser_id FROM #merge_mapping);

-- ============================================================================
-- STEP 4: MIGRATE AND DEDUPLICATE HEM ASSOCIATIONS (ENG-1973 PREPARATION)
-- ============================================================================

-- CRITICAL: Ensures (vup_id, hem) uniqueness constraint for future email table enhancement

-- Build comprehensive email migration plan with conflict resolution
DROP TABLE IF EXISTS #email_migration_plan;

CREATE TEMP TABLE #email_migration_plan AS
SELECT 
    ve.vup_id as current_vup_id,
    mm.winner_id as target_vup_id,
    ve.sha256 as hem,
    ve.email,
    ve.email_type,
    ve.data_source,
    ve.dataset_version,
    CASE 
        -- Winner already has this exact HEM: mark for deletion
        WHEN EXISTS (
            SELECT 1 FROM derived.vector_email existing
            WHERE existing.vup_id = mm.winner_id
              AND existing.sha256 = ve.sha256
        ) THEN 'DELETE'
        -- Winner does not have this HEM: mark for migration
        ELSE 'MIGRATE'
    END as action,
    -- Priority ranking for conflict resolution (when multiple losers have same HEM)
    -- Prioritize: business emails > emails with cleartext > most recent > lowest vup_id
    ROW_NUMBER() OVER (
        PARTITION BY ve.sha256, mm.winner_id 
        ORDER BY 
            CASE WHEN ve.email_type = 'business' THEN 1 
                 WHEN ve.email_type = 'personal' THEN 2 
                 ELSE 3 END,
            CASE WHEN ve.email IS NOT NULL AND TRIM(ve.email) != '' THEN 1 ELSE 2 END,
            LENGTH(COALESCE(ve.email, '')) DESC,
            ve.vup_id ASC
    ) as priority_rank_for_hem
FROM derived.vector_email ve
INNER JOIN #merge_mapping mm ON ve.vup_id = mm.loser_id
WHERE ve.sha256 IS NOT NULL
  AND TRIM(ve.sha256) != ''
  AND LENGTH(TRIM(ve.sha256)) = 64;

-- Display email migration plan summary
SELECT 
    'Email Migration Plan Summary' as summary_type,
    action,
    COUNT(*) as email_records,
    COUNT(DISTINCT hem) as unique_hems,
    COUNT(DISTINCT current_vup_id) as source_persons,
    COUNT(DISTINCT target_vup_id) as target_persons,
    SUM(CASE WHEN email IS NOT NULL AND TRIM(email) != '' THEN 1 ELSE 0 END) as records_with_cleartext
FROM #email_migration_plan
GROUP BY action;

-- Execute email record migration (only highest priority record per HEM)
UPDATE derived.vector_email
SET vup_id = emp.target_vup_id,
    updated_at = GETDATE()
FROM #email_migration_plan emp
WHERE derived.vector_email.vup_id = emp.current_vup_id
  AND derived.vector_email.sha256 = emp.hem
  AND emp.action = 'MIGRATE'
  AND emp.priority_rank_for_hem = 1;  -- Only migrate highest priority record

-- Delete duplicate email records (where winner already has this HEM)
DELETE FROM derived.vector_email
WHERE EXISTS (
    SELECT 1 FROM #email_migration_plan emp
    WHERE derived.vector_email.vup_id = emp.current_vup_id
      AND derived.vector_email.sha256 = emp.hem
      AND emp.action = 'DELETE'
);

-- Delete remaining lower-priority duplicates from migration
DELETE FROM derived.vector_email
WHERE EXISTS (
    SELECT 1 FROM #email_migration_plan emp
    WHERE derived.vector_email.vup_id = emp.current_vup_id
      AND derived.vector_email.sha256 = emp.hem
      AND emp.action = 'MIGRATE'
      AND emp.priority_rank_for_hem > 1
);

-- Verify HEM uniqueness constraint (critical for ENG-1973)
SELECT 
    'HEM Uniqueness Verification (ENG-1973 Preparation)' as check_type,
    COUNT(*) as total_email_records_on_winners,
    COUNT(DISTINCT sha256) as unique_hems_on_winners,
    COUNT(*) - COUNT(DISTINCT sha256) as duplicate_hem_instances,
    COUNT(DISTINCT vup_id || '::' || sha256) as unique_vup_hem_pairs,
    CASE 
        WHEN COUNT(*) = COUNT(DISTINCT vup_id || '::' || sha256)
        THEN 'SUCCESS: (vup_id, hem) uniqueness established - Ready for ENG-1973'
        ELSE 'WARNING: Duplicate (vup_id, hem) pairs still exist - Investigation required'
    END as uniqueness_status
FROM derived.vector_email
WHERE vup_id IN (SELECT winner_id FROM #merge_mapping)
  AND sha256 IS NOT NULL;

-- ============================================================================
-- STEP 5: MIGRATE JOB RECORDS FROM LOSERS TO WINNERS
-- ============================================================================

UPDATE derived.vector_universal_job
SET vup_id = mm.winner_id,
    updated_at = GETDATE()
FROM #merge_mapping mm
WHERE derived.vector_universal_job.vup_id = mm.loser_id;

-- Verify job migration completion
SELECT 
    'Job Migration Verification' as check_type,
    (SELECT COUNT(*) FROM derived.vector_universal_job 
     WHERE vup_id IN (SELECT loser_id FROM #merge_mapping)) as jobs_remaining_on_losers,
    (SELECT COUNT(*) FROM derived.vector_universal_job 
     WHERE vup_id IN (SELECT winner_id FROM #merge_mapping)) as jobs_now_on_winners,
    CASE 
        WHEN (SELECT COUNT(*) FROM derived.vector_universal_job 
              WHERE vup_id IN (SELECT loser_id FROM #merge_mapping)) = 0
        THEN 'JOB MIGRATION SUCCESSFUL - No orphans created'
        ELSE 'WARNING - Jobs still remain on loser records'
    END as migration_status;

-- ============================================================================
-- STEP 6: MARK LOSER PERSON RECORDS AS MERGED (SOFT DELETE PATTERN)
-- ============================================================================

UPDATE derived.vector_universal_person
SET merged_into_vup_id = mm.winner_id,
    updated_at = GETDATE()
FROM #merge_mapping mm
WHERE derived.vector_universal_person.vup_id = mm.loser_id;

-- Verify merge marking completion
SELECT 
    'Merge Marking Verification' as check_type,
    COUNT(*) as losers_successfully_marked,
    (SELECT COUNT(DISTINCT loser_id) FROM #merge_mapping) as expected_losers,
    (SELECT COUNT(*) FROM derived.vector_universal_person 
     WHERE merged_into_vup_id IS NOT NULL) as total_merged_records_in_table,
    CASE 
        WHEN COUNT(*) = (SELECT COUNT(DISTINCT loser_id) FROM #merge_mapping)
        THEN 'ALL LOSERS MARKED SUCCESSFULLY'
        ELSE 'WARNING - Marking incomplete, investigate discrepancy'
    END as marking_status
FROM derived.vector_universal_person
WHERE vup_id IN (SELECT loser_id FROM #merge_mapping)
  AND merged_into_vup_id IS NOT NULL;

-- ============================================================================
-- STEP 7: COMPREHENSIVE ORPHAN CHECK (SHOULD BE ZERO)
-- ============================================================================

SELECT 
    'Final Orphan Check' as validation_type,
    (SELECT COUNT(*) 
     FROM derived.vector_universal_job vuj 
     LEFT JOIN derived.vector_universal_person vup ON vuj.vup_id = vup.vup_id
     WHERE vup.vup_id IS NULL) as orphaned_jobs,
    (SELECT COUNT(*) 
     FROM derived.vector_email ve 
     LEFT JOIN derived.vector_universal_person vup ON ve.vup_id = vup.vup_id
     WHERE vup.vup_id IS NULL AND ve.vup_id IS NOT NULL) as orphaned_emails,
    CASE 
        WHEN (SELECT COUNT(*) 
              FROM derived.vector_universal_job vuj 
              LEFT JOIN derived.vector_universal_person vup ON vuj.vup_id = vup.vup_id
              WHERE vup.vup_id IS NULL) = 0
         AND (SELECT COUNT(*) 
              FROM derived.vector_email ve 
              LEFT JOIN derived.vector_universal_person vup ON ve.vup_id = vup.vup_id
              WHERE vup.vup_id IS NULL AND ve.vup_id IS NOT NULL) = 0
        THEN 'NO ORPHANS CREATED - Migration maintained referential integrity'
        ELSE 'ORPHANS DETECTED - ROLLBACK REQUIRED - DO NOT COMMIT'
    END as orphan_status;

-- ============================================================================
-- STEP 8: POST-MERGE DATA QUALITY VERIFICATION
-- ============================================================================

SELECT 
    'Data Quality Verification' as check_type,
    (SELECT COUNT(*) FROM derived.vector_universal_person 
     WHERE merged_into_vup_id IS NULL) as active_person_records,
    (SELECT COUNT(*) FROM derived.vector_universal_person 
     WHERE merged_into_vup_id IS NOT NULL) as merged_person_records,
    (SELECT COUNT(*) FROM derived.vector_universal_job) as total_job_records,
    (SELECT COUNT(DISTINCT vup_id) FROM derived.vector_universal_job) as unique_persons_with_jobs,
    (SELECT COUNT(*) FROM derived.vector_email WHERE vup_id IS NOT NULL) as total_email_records,
    (SELECT COUNT(DISTINCT vup_id) FROM derived.vector_email WHERE vup_id IS NOT NULL) as unique_persons_with_emails,
    (SELECT COUNT(DISTINCT vup_id || '::' || sha256) FROM derived.vector_email 
     WHERE sha256 IS NOT NULL) as unique_vup_hem_combinations;

-- ============================================================================
-- STEP 9: CALCULATE DEDUPLICATION IMPACT METRICS
-- ============================================================================

SELECT 
    'Deduplication Impact Summary' as summary_type,
    (SELECT COUNT(*) FROM derived.vector_universal_person_backup_eng1914) as original_person_count,
    (SELECT COUNT(*) FROM derived.vector_universal_person 
     WHERE merged_into_vup_id IS NULL) as current_active_person_count,
    (SELECT COUNT(*) FROM derived.vector_universal_person 
     WHERE merged_into_vup_id IS NOT NULL) as persons_merged_away,
    (SELECT COUNT(*) FROM derived.vector_universal_person_backup_eng1914) - 
    (SELECT COUNT(*) FROM derived.vector_universal_person 
     WHERE merged_into_vup_id IS NULL) as total_reduction,
    ROUND(100.0 * (
        (SELECT COUNT(*) FROM derived.vector_universal_person_backup_eng1914) - 
        (SELECT COUNT(*) FROM derived.vector_universal_person WHERE merged_into_vup_id IS NULL)
    ) / NULLIF((SELECT COUNT(*) FROM derived.vector_universal_person_backup_eng1914), 0), 2) as pct_reduction,
    (SELECT COUNT(DISTINCT group_id) FROM #vup_duplicate_groups) as duplicate_groups_processed,
    'Merge operation successfully consolidated duplicate person records' as operation_status;

-- ============================================================================
-- STEP 10: GENERATE COMPREHENSIVE MERGE COMPLETION REPORT
-- ============================================================================

SELECT 
    'Part 4 Merge Operation Complete' as summary_type,
    (SELECT COUNT(DISTINCT group_id) FROM #merge_mapping) as groups_merged,
    (SELECT COUNT(DISTINCT winner_id) FROM #merge_mapping) as unique_winners_retained,
    (SELECT COUNT(DISTINCT loser_id) FROM #merge_mapping) as unique_losers_merged,
    (SELECT COUNT(*) FROM derived.vector_universal_person 
     WHERE merged_into_vup_id IS NULL) as final_active_person_count,
    (SELECT COUNT(*) FROM derived.vector_universal_person 
     WHERE merged_into_vup_id IS NOT NULL) as final_merged_person_count,
    (SELECT COUNT(DISTINCT vup_id || '::' || sha256) FROM derived.vector_email 
     WHERE sha256 IS NOT NULL) = 
    (SELECT COUNT(*) FROM derived.vector_email 
     WHERE sha256 IS NOT NULL) as hem_uniqueness_achieved,
    'Review all verification queries above before committing transaction' as critical_reminder,
    'Transaction ready for COMMIT only if all verifications show SUCCESS' as commit_guidance,
    GETDATE() as merge_completed_at;

*/

-- ============================================================================
-- COMMIT DECISION CHECKPOINT
-- Execute COMMIT only after verifying all checks above show SUCCESS
-- Execute ROLLBACK immediately if any verification shows WARNING or ERROR
-- ============================================================================

SELECT 
    'Transaction Decision Required' as decision_point,
    'Step 1: Review merge plan summary thoroughly' as instruction_1,
    'Step 2: Uncomment and execute merge operations sections' as instruction_2,
    'Step 3: Review ALL verification query results carefully' as instruction_3,
    'Step 4: Verify HEM uniqueness status shows SUCCESS' as instruction_4,
    'Step 5: Verify orphan check shows zero orphans' as instruction_5,
    'Step 6: Execute COMMIT if all verifications pass' as instruction_6,
    'Step 7: Execute ROLLBACK immediately if any issues detected' as instruction_7;

-- COMMIT;  -- Execute ONLY after all verifications pass
-- ROLLBACK;  -- Execute immediately if any verification fails

