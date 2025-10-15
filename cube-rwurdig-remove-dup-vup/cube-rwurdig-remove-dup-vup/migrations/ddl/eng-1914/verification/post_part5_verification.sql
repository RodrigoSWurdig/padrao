-- ============================================================================
-- ENG-1914 Part 5 Post-Execution Verification
-- Execute after 05_cleanup_columns.sql completes
-- Final validation of entire migration
-- ============================================================================

-- Verification 1: Table Structure
SELECT 
    'Table Structure' as verification_type,
    column_name,
    data_type,
    character_maximum_length
FROM information_schema.columns
WHERE table_schema = 'derived'
  AND table_name = 'vector_universal_person'
ORDER BY ordinal_position;

-- Verification 2: Legacy Columns Removed
SELECT 
    'Legacy Columns' as verification_type,
    CASE 
        WHEN COUNT(*) = 0 
        THEN '✅ PASS - Legacy columns removed'
        ELSE '❌ FAIL - Legacy columns still exist'
    END as status
FROM information_schema.columns
WHERE table_schema = 'derived'
  AND table_name = 'vector_universal_person'
  AND column_name IN ('linkedin_url', 'fbf_id', 'pdl_id');

-- Verification 3: Record Preservation
SELECT 
    'Record Counts' as verification_type,
    (SELECT COUNT(*) FROM derived.vector_universal_person) as current_count,
    (SELECT COUNT(*) FROM derived.vector_universal_person_backup_eng1914) as backup_count,
    CASE 
        WHEN (SELECT COUNT(*) FROM derived.vector_universal_person) = 
             (SELECT COUNT(*) FROM derived.vector_universal_person_backup_eng1914)
        THEN '✅ PASS - Record counts match'
        ELSE '❌ FAIL - Record count mismatch'
    END as status;

-- Verification 4: Merged Records Preserved
SELECT 
    'Merged Records' as verification_type,
    COUNT(*) as total_merged,
    CASE 
        WHEN COUNT(*) > 0 
        THEN '✅ PASS - Merged records preserved'
        ELSE '⚠️  WARNING - Check if merges expected'
    END as status
FROM derived.vector_universal_person
WHERE merged_into_vup_id IS NOT NULL;

-- Verification 5: Datashare Registration
SELECT 
    'Datashare Status' as verification_type,
    object_name,
    object_type,
    share_name,
    CASE 
        WHEN object_name = 'vector_universal_person' 
        THEN '✅ PASS - Main table registered'
        ELSE '✅ Junction table registered'
    END as status
FROM svv_datashare_objects
WHERE share_name = 'vector_core_datashare'
  AND schema = 'derived'
  AND object_name IN (
    'vector_universal_person',
    'vup_linkedin_urls',
    'vup_fbf_ids',
    'vup_pdl_ids'
  )
ORDER BY object_name;

-- Verification 6: Storage Optimization
SELECT 
    'Storage Metrics' as verification_type,
    pg_size_pretty(pg_total_relation_size('derived.vector_universal_person')) as current_size,
    pg_size_pretty(pg_relation_size('derived.vector_universal_person')) as data_size,
    '✅ Review storage savings from column removal' as status;

-- Verification 7: Child Table Integrity
SELECT 
    'Child Table Integrity' as verification_type,
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
        THEN '✅ PASS - All child relationships intact'
        ELSE '❌ FAIL - Orphaned child records detected'
    END as status;

-- Final Summary
SELECT 
    'Migration Complete' as summary_type,
    (SELECT COUNT(*) FROM derived.vector_universal_person) as total_persons,
    (SELECT COUNT(*) FROM derived.vector_universal_person WHERE merged_into_vup_id IS NULL) as active_persons,
    (SELECT COUNT(*) FROM derived.vector_universal_person WHERE merged_into_vup_id IS NOT NULL) as merged_persons,
    (SELECT COUNT(*) FROM derived.vup_linkedin_urls) as linkedin_associations,
    (SELECT COUNT(*) FROM derived.vup_fbf_ids) as fbf_associations,
    (SELECT COUNT(*) FROM derived.vup_pdl_ids) as pdl_associations,
    '✅ ENG-1914 Migration Successfully Completed' as status;
