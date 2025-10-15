-- ============================================================================
-- ENG-1914 Part 4 Post-Execution Verification
-- Execute after 04_merge_duplicates.sql completes
-- All checks must pass before Part 5
-- ============================================================================

-- Verification 1: Soft Delete Implementation
SELECT 
    'Soft Deletes' as verification_type,
    COUNT(*) as total_records,
    COUNT(CASE WHEN merged_into_vup_id IS NULL THEN 1 END) as active_records,
    COUNT(CASE WHEN merged_into_vup_id IS NOT NULL THEN 1 END) as merged_records,
    ROUND(100.0 * COUNT(CASE WHEN merged_into_vup_id IS NOT NULL THEN 1 END) / COUNT(*), 2) as pct_merged,
    CASE 
        WHEN COUNT(CASE WHEN merged_into_vup_id IS NOT NULL THEN 1 END) > 0
        THEN '✅ PASS - Soft deletes implemented'
        ELSE '❌ FAIL - No merged records found'
    END as status
FROM derived.vector_universal_person;

-- Verification 2: Child Table Orphans Check
SELECT 
    'Child Table Orphans' as verification_type,
    (SELECT COUNT(*) FROM derived.vector_universal_job vuj 
     WHERE NOT EXISTS (SELECT 1 FROM derived.vector_universal_person vup 
                       WHERE vup.vup_id = vuj.vup_id AND vup.merged_into_vup_id IS NULL)) as orphaned_jobs,
    (SELECT COUNT(*) FROM derived.vector_email ve 
     WHERE ve.vup_id IS NOT NULL 
       AND NOT EXISTS (SELECT 1 FROM derived.vector_universal_person vup 
                       WHERE vup.vup_id = ve.vup_id AND vup.merged_into_vup_id IS NULL)) as orphaned_emails,
    (SELECT COUNT(*) FROM derived.vector_maid vm 
     WHERE NOT EXISTS (SELECT 1 FROM derived.vector_universal_person vup 
                       WHERE vup.vup_id = vm.vup_id AND vup.merged_into_vup_id IS NULL)) as orphaned_maids,
    (SELECT COUNT(*) FROM derived.vector_phone vp 
     WHERE NOT EXISTS (SELECT 1 FROM derived.vector_universal_person vup 
                       WHERE vup.vup_id = vp.vup_id AND vup.merged_into_vup_id IS NULL)) as orphaned_phones,
    CASE 
        WHEN (SELECT COUNT(*) FROM derived.vector_universal_job vuj 
              WHERE NOT EXISTS (SELECT 1 FROM derived.vector_universal_person vup 
                                WHERE vup.vup_id = vuj.vup_id AND vup.merged_into_vup_id IS NULL)) = 0
         AND (SELECT COUNT(*) FROM derived.vector_email ve 
              WHERE ve.vup_id IS NOT NULL 
                AND NOT EXISTS (SELECT 1 FROM derived.vector_universal_person vup 
                                WHERE vup.vup_id = ve.vup_id AND vup.merged_into_vup_id IS NULL)) = 0
         AND (SELECT COUNT(*) FROM derived.vector_maid vm 
              WHERE NOT EXISTS (SELECT 1 FROM derived.vector_universal_person vup 
                                WHERE vup.vup_id = vm.vup_id AND vup.merged_into_vup_id IS NULL)) = 0
         AND (SELECT COUNT(*) FROM derived.vector_phone vp 
              WHERE NOT EXISTS (SELECT 1 FROM derived.vector_universal_person vup 
                                WHERE vup.vup_id = vp.vup_id AND vup.merged_into_vup_id IS NULL)) = 0
        THEN '✅ PASS - Zero orphaned records'
        ELSE '❌ FAIL - Orphaned records exist'
    END as status;

-- Verification 3: Junction Table Cleanup
SELECT 
    'Junction Table Cleanup' as verification_type,
    (SELECT COUNT(*) FROM derived.vup_linkedin_urls WHERE vup_id IN 
        (SELECT vup_id FROM derived.vector_universal_person WHERE merged_into_vup_id IS NOT NULL)) as linkedin_losers,
    (SELECT COUNT(*) FROM derived.vup_fbf_ids WHERE vup_id IN 
        (SELECT vup_id FROM derived.vector_universal_person WHERE merged_into_vup_id IS NOT NULL)) as fbf_losers,
    (SELECT COUNT(*) FROM derived.vup_pdl_ids WHERE vup_id IN 
        (SELECT vup_id FROM derived.vector_universal_person WHERE merged_into_vup_id IS NOT NULL)) as pdl_losers,
    CASE 
        WHEN (SELECT COUNT(*) FROM derived.vup_linkedin_urls WHERE vup_id IN 
                (SELECT vup_id FROM derived.vector_universal_person WHERE merged_into_vup_id IS NOT NULL)) = 0
         AND (SELECT COUNT(*) FROM derived.vup_fbf_ids WHERE vup_id IN 
                (SELECT vup_id FROM derived.vector_universal_person WHERE merged_into_vup_id IS NOT NULL)) = 0
         AND (SELECT COUNT(*) FROM derived.vup_pdl_ids WHERE vup_id IN 
                (SELECT vup_id FROM derived.vector_universal_person WHERE merged_into_vup_id IS NOT NULL)) = 0
        THEN '✅ PASS - Junction tables cleaned'
        ELSE '❌ FAIL - Loser identifiers remain'
    END as status;

-- Verification 4: Winner Identifier Migration
SELECT 
    'Winner Identifiers' as verification_type,
    (SELECT COUNT(DISTINCT vup_id) FROM derived.vup_linkedin_urls) as unique_linkedin_persons,
    (SELECT COUNT(DISTINCT vup_id) FROM derived.vup_fbf_ids) as unique_fbf_persons,
    (SELECT COUNT(DISTINCT vup_id) FROM derived.vup_pdl_ids) as unique_pdl_persons,
    '✅ Review identifier consolidation' as status;

-- Verification 5: Merge Statistics
SELECT 
    'Merge Statistics' as verification_type,
    (SELECT COUNT(*) FROM derived.vector_universal_person WHERE merged_into_vup_id IS NULL) as active_count,
    (SELECT COUNT(*) FROM derived.vector_universal_person WHERE merged_into_vup_id IS NOT NULL) as merged_count,
    (SELECT COUNT(*) FROM derived.vector_universal_person) as total_count,
    '✅ Document statistics for business review' as status;

-- Summary
SELECT 
    'Part 4 Summary' as summary_type,
    'Duplicate merge completed successfully' as message,
    '⚠️  CRITICAL: Verify cube schemas deployed before Part 5' as next_step;
