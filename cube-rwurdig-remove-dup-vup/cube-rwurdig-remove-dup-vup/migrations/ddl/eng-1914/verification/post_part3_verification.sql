-- ============================================================================
-- ENG-1914 Part 3 Post-Execution Verification
-- Execute after 03_identify_duplicates.sql completes
-- All checks must pass before proceeding to Part 4
-- ============================================================================

-- Verification 1: Duplicate Edges Created
SELECT 
    'Duplicate Edges' as verification_type,
    COUNT(*) as total_edges,
    COUNT(DISTINCT identifier_type) as identifier_types,
    CASE 
        WHEN COUNT(*) > 0 
        THEN '✅ PASS - Duplicate edges identified'
        ELSE '⚠️  WARNING - No duplicates found'
    END as status
FROM duplicate_edges;

-- Verification 2: Duplicate Groups Summary
SELECT 
    'Duplicate Groups' as verification_type,
    COUNT(DISTINCT group_id) as total_groups,
    COUNT(*) as total_vup_ids,
    COUNT(*) - COUNT(DISTINCT group_id) as vups_to_merge,
    CASE 
        WHEN COUNT(DISTINCT group_id) > 0 
        THEN '✅ PASS - Groups identified'
        ELSE '⚠️  WARNING - No duplicate groups'
    END as status
FROM vup_duplicate_groups;

-- Verification 3: Largest Groups Review
SELECT 
    'Largest Groups' as verification_type,
    group_id,
    group_size
FROM (
    SELECT 
        group_id,
        COUNT(*) as group_size
    FROM vup_duplicate_groups
    GROUP BY group_id
    ORDER BY group_size DESC
    LIMIT 10
) x;

-- Verification 4: Identifier Type Distribution
SELECT 
    'Identifier Distribution' as verification_type,
    identifier_type,
    COUNT(*) as edge_count,
    COUNT(DISTINCT vup_id_1) + COUNT(DISTINCT vup_id_2) as affected_vups
FROM duplicate_edges
GROUP BY identifier_type
ORDER BY edge_count DESC;

-- Verification 5: Transitive Closure Validation
SELECT 
    'Transitive Closure' as verification_type,
    'Verify largest groups represent true duplicates' as note,
    '✅ Review samples before proceeding to merge' as status;

-- Summary
SELECT 
    'Part 3 Summary' as summary_type,
    (SELECT COUNT(DISTINCT group_id) FROM vup_duplicate_groups) as groups_identified,
    (SELECT COUNT(*) - COUNT(DISTINCT group_id) FROM vup_duplicate_groups) as records_to_merge,
    '⚠️  CRITICAL: Review duplicate patterns before Part 4' as next_step;
