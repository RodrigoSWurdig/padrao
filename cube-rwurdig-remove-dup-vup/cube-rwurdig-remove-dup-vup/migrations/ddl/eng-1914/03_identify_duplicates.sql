-- ============================================================================
-- ENG-1914 Part 3: Identify Duplicate Person Records via Multi-Identifier Analysis
-- Purpose: Detect duplicates using LinkedIn URLs, FBF IDs, PDL IDs, and HEMs
-- Operations: Read-only analysis creating temporary tables for merge planning
-- Expected Results: ~3M duplicate instances identified for consolidation
-- Prerequisites: Part 2 completed, all junction tables populated and verified
-- ============================================================================

-- Pre-execution verification checkpoint
SELECT 
    'Pre-Analysis Verification' as checkpoint_type,
    (SELECT COUNT(*) FROM derived.vup_linkedin_urls) as linkedin_urls_available,
    (SELECT COUNT(*) FROM derived.vup_fbf_ids) as fbf_ids_available,
    (SELECT COUNT(*) FROM derived.vup_pdl_ids) as pdl_ids_available,
    (SELECT COUNT(*) FROM derived.vector_email WHERE sha256 IS NOT NULL AND vup_id IS NOT NULL) as hems_available,
    CASE 
        WHEN (SELECT COUNT(*) FROM derived.vup_linkedin_urls) > 0
         AND (SELECT COUNT(*) FROM derived.vup_fbf_ids) > 0
         AND (SELECT COUNT(*) FROM derived.vup_pdl_ids) > 0
         AND (SELECT COUNT(*) FROM derived.vector_email WHERE sha256 IS NOT NULL AND vup_id IS NOT NULL) > 0
        THEN 'READY - All identifier sources populated'
        ELSE 'ERROR - Missing identifier data, verify Part 2 completion'
    END as readiness_status,
    GETDATE() as checkpoint_time;

-- ============================================================================
-- STEP 1: BUILD COMPREHENSIVE IDENTIFIER CATALOG
-- ============================================================================

-- Build comprehensive identifier catalog from all four sources
DROP TABLE IF EXISTS #duplicate_identifiers;

(
  -- LinkedIn URL pairs (same URL = duplicate)
  SELECT DISTINCT
    'linkedin_url' as identifier_type,
    l1.vup_id as vup_id_1,
    l2.vup_id as vup_id_2,
    l1.linkedin_url as identifier_value
  FROM derived.vup_linkedin_urls l1
  INNER JOIN derived.vup_linkedin_urls l2
    ON l1.linkedin_url = l2.linkedin_url
   AND l1.vup_id < l2.vup_id  -- prevent self-pairs and duplicates
  
  UNION ALL
  
  -- FBF ID pairs
  SELECT DISTINCT
    'fbf_id' as identifier_type,
    f1.vup_id as vup_id_1,
    f2.vup_id as vup_id_2,
    f1.fbf_id as identifier_value
  FROM derived.vup_fbf_ids f1
  INNER JOIN derived.vup_fbf_ids f2
    ON f1.fbf_id = f2.fbf_id
   AND f1.vup_id < f2.vup_id
  
  UNION ALL
  
  -- PDL ID pairs
  SELECT DISTINCT
    'pdl_id' as identifier_type,
    p1.vup_id as vup_id_1,
    p2.vup_id as vup_id_2,
    p1.pdl_id as identifier_value
  FROM derived.vup_pdl_ids p1
  INNER JOIN derived.vup_pdl_ids p2
    ON p1.pdl_id = p2.pdl_id
   AND p1.vup_id < p2.vup_id

  UNION ALL

  -- HEM (sha256) pairs from vector_email
  -- Critical for ENG-1973 email consolidation preparation
  SELECT DISTINCT
    'hem_sha256' as identifier_type,
    e1.vup_id as vup_id_1,
    e2.vup_id as vup_id_2,
    e1.sha256 as identifier_value
  FROM derived.vector_email e1
  INNER JOIN derived.vector_email e2
    ON e1.sha256 = e2.sha256
   AND e1.vup_id < e2.vup_id
  WHERE e1.sha256 IS NOT NULL
    AND e2.sha256 IS NOT NULL
    AND e1.vup_id IS NOT NULL
    AND e2.vup_id IS NOT NULL
);

-- Verify identifier catalog construction
SELECT 
    'Identifier Catalog Built' as step_result,
    COUNT(*) as total_duplicate_pairs,
    COUNT(DISTINCT identifier_type) as identifier_types_detected,
    COUNT(DISTINCT vup_id_1) + COUNT(DISTINCT vup_id_2) as vup_ids_involved,
    GETDATE() as step_timestamp
FROM #duplicate_identifiers;

-- ============================================================================
-- STEP 2: BUILD DUPLICATE EDGES GRAPH
-- ============================================================================

-- Create duplicate edges from identifier catalog
-- These edges represent the graph for transitive closure analysis
DROP TABLE IF EXISTS #duplicate_edges;

CREATE TEMP TABLE #duplicate_edges AS
SELECT DISTINCT
    vup_id_1,
    vup_id_2,
    identifier_type,
    identifier_value
FROM #duplicate_identifiers;

-- Verify edge construction
SELECT 
    'Duplicate Edges Summary' as summary_type,
    identifier_type,
    COUNT(*) as duplicate_pair_count,
    COUNT(DISTINCT vup_id_1) + COUNT(DISTINCT vup_id_2) as affected_vup_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage_of_edges
FROM #duplicate_edges
GROUP BY identifier_type
ORDER BY duplicate_pair_count DESC;

-- ============================================================================
-- STEP 3: BUILD DUPLICATE GROUPS VIA TRANSITIVE CLOSURE
-- ============================================================================

-- Apply Union-Find algorithm via recursive CTE to find connected components
-- If vup_id A shares identifier with B, and B shares identifier with C,
-- then A, B, C are all duplicates even if A and C don't directly share an identifier
-- Iteration limit prevents infinite recursion on pathological cases
DROP TABLE IF EXISTS #vup_duplicate_groups;

CREATE TEMP TABLE #vup_duplicate_groups AS
WITH RECURSIVE transitive_closure AS (
    -- Base case: Initialize each vup_id with itself as provisional group_id
    SELECT DISTINCT
        vup_id_1 as vup_id,
        vup_id_1 as group_id,
        0 as iteration
    FROM #duplicate_edges
    
    UNION
    
    SELECT DISTINCT
        vup_id_2 as vup_id,
        vup_id_2 as group_id,
        0 as iteration
    FROM #duplicate_edges
    
    UNION ALL
    
    -- Recursive case: Propagate minimum group_id through connected edges
    -- Each iteration expands the closure by one hop in the duplicate graph
    SELECT 
        tc.vup_id,
        CASE 
            WHEN de.vup_id_1 < tc.group_id THEN de.vup_id_1
            WHEN de.vup_id_2 < tc.group_id THEN de.vup_id_2
            ELSE tc.group_id
        END as group_id,
        tc.iteration + 1 as iteration
    FROM transitive_closure tc
    INNER JOIN #duplicate_edges de 
        ON tc.vup_id IN (de.vup_id_1, de.vup_id_2)
    WHERE tc.iteration < 15  -- Limit iterations to prevent runaway recursion
      AND CASE 
            WHEN de.vup_id_1 < tc.group_id THEN de.vup_id_1
            WHEN de.vup_id_2 < tc.group_id THEN de.vup_id_2
            ELSE tc.group_id
          END < tc.group_id  -- Only continue if group_id is still changing
)
-- Take minimum group_id for each vup_id (ensures all connected nodes get same group)
SELECT 
    vup_id,
    MIN(group_id) as group_id,
    MAX(iteration) as max_iteration_reached
FROM transitive_closure
GROUP BY vup_id;

-- Verify transitive closure convergence
SELECT 
    'Transitive Closure Complete' as step_result,
    COUNT(DISTINCT vup_id) as vup_ids_in_groups,
    COUNT(DISTINCT group_id) as total_groups,
    MAX(max_iteration_reached) as deepest_iteration,
    CASE 
        WHEN MAX(max_iteration_reached) >= 14 THEN 'WARNING - Iteration limit reached, check for pathological clusters'
        ELSE 'OK - Convergence achieved'
    END as convergence_status,
    GETDATE() as step_timestamp
FROM #vup_duplicate_groups;

-- ============================================================================
-- STEP 4: GROUP SIZE DISTRIBUTION ANALYSIS
-- ============================================================================

-- Understand the distribution of duplicate group sizes
-- Most groups should be pairs (size=2), larger groups indicate data quality issues
WITH group_sizes AS (
    SELECT 
        group_id,
        COUNT(*) as group_size
    FROM #vup_duplicate_groups
    GROUP BY group_id
)
SELECT 
    'Group Size Distribution' as analysis_type,
    group_size,
    COUNT(*) as group_count,
    SUM(group_size) as total_vup_ids_in_size,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as pct_of_groups,
    ROUND(100.0 * SUM(group_size) / SUM(SUM(group_size)) OVER(), 2) as pct_of_vup_ids
FROM group_sizes
GROUP BY group_size
ORDER BY group_size;

-- High-level summary statistics
WITH group_sizes AS (
    SELECT 
        group_id,
        COUNT(*) as group_size
    FROM #vup_duplicate_groups
    GROUP BY group_id
)
SELECT 
    'Group Summary Statistics' as summary_type,
    COUNT(DISTINCT group_id) as total_groups,
    SUM(group_size) as total_duplicate_vup_ids,
    SUM(group_size) - COUNT(DISTINCT group_id) as vup_ids_to_consolidate,
    ROUND(AVG(group_size), 2) as avg_group_size,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY group_size) as median_group_size,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY group_size) as p95_group_size,
    MAX(group_size) as max_group_size
FROM group_sizes;

-- ============================================================================
-- STEP 5: IDENTIFIER COMBINATION PATTERNS
-- ============================================================================

-- Analyze which combinations of identifier types drive duplicate detection
-- This reveals cross-source data quality patterns and identifier reliability
WITH group_identifiers AS (
    SELECT 
        vdg.group_id,
        MAX(CASE WHEN de.identifier_type = 'linkedin_url' THEN 1 ELSE 0 END) as has_linkedin,
        MAX(CASE WHEN de.identifier_type = 'fbf_id' THEN 1 ELSE 0 END) as has_fbf,
        MAX(CASE WHEN de.identifier_type = 'pdl_id' THEN 1 ELSE 0 END) as has_pdl,
        MAX(CASE WHEN de.identifier_type = 'hem_sha256' THEN 1 ELSE 0 END) as has_hem,
        COUNT(*) as group_size
    FROM #vup_duplicate_groups vdg
    LEFT JOIN #duplicate_edges de 
        ON vdg.vup_id IN (de.vup_id_1, de.vup_id_2)
    GROUP BY vdg.group_id
)
SELECT 
    'Identifier Combination Patterns' as analysis_type,
    CASE 
        WHEN has_linkedin = 1 AND has_fbf = 1 AND has_pdl = 1 AND has_hem = 1 THEN 'All 4 Types (LinkedIn+FBF+PDL+HEM)'
        WHEN has_linkedin = 1 AND has_fbf = 1 AND has_pdl = 1 THEN 'LinkedIn + FBF + PDL'
        WHEN has_linkedin = 1 AND has_hem = 1 THEN 'LinkedIn + HEM'
        WHEN has_fbf = 1 AND has_hem = 1 THEN 'FBF + HEM'
        WHEN has_pdl = 1 AND has_hem = 1 THEN 'PDL + HEM'
        WHEN has_linkedin = 1 AND has_fbf = 1 THEN 'LinkedIn + FBF'
        WHEN has_linkedin = 1 THEN 'LinkedIn Only'
        WHEN has_fbf = 1 THEN 'FBF Only'
        WHEN has_pdl = 1 THEN 'PDL Only'
        WHEN has_hem = 1 THEN 'HEM Only'
        ELSE 'Unknown'
    END as identifier_pattern,
    COUNT(DISTINCT group_id) as group_count,
    SUM(group_size) as total_vup_ids,
    ROUND(100.0 * COUNT(DISTINCT group_id) / SUM(COUNT(DISTINCT group_id)) OVER(), 2) as pct_of_groups
FROM group_identifiers
GROUP BY 
    CASE 
        WHEN has_linkedin = 1 AND has_fbf = 1 AND has_pdl = 1 AND has_hem = 1 THEN 'All 4 Types (LinkedIn+FBF+PDL+HEM)'
        WHEN has_linkedin = 1 AND has_fbf = 1 AND has_pdl = 1 THEN 'LinkedIn + FBF + PDL'
        WHEN has_linkedin = 1 AND has_hem = 1 THEN 'LinkedIn + HEM'
        WHEN has_fbf = 1 AND has_hem = 1 THEN 'FBF + HEM'
        WHEN has_pdl = 1 AND has_hem = 1 THEN 'PDL + HEM'
        WHEN has_linkedin = 1 AND has_fbf = 1 THEN 'LinkedIn + FBF'
        WHEN has_linkedin = 1 THEN 'LinkedIn Only'
        WHEN has_fbf = 1 THEN 'FBF Only'
        WHEN has_pdl = 1 THEN 'PDL Only'
        WHEN has_hem = 1 THEN 'HEM Only'
        ELSE 'Unknown'
    END
ORDER BY group_count DESC;

-- ============================================================================
-- STEP 6: HEM-SPECIFIC DUPLICATE ANALYSIS
-- ============================================================================

-- Deep dive into HEM (email hash) duplicate patterns
-- Critical for ENG-1973 email table consolidation planning
WITH hem_duplicates AS (
    SELECT 
        de.identifier_value as hem_sha256,
        de.vup_id_1,
        de.vup_id_2,
        e1.email_type as vup1_email_type,
        e2.email_type as vup2_email_type,
        e1.created_at as vup1_email_created,
        e2.created_at as vup2_email_created
    FROM #duplicate_edges de
    LEFT JOIN derived.vector_email e1 
        ON de.vup_id_1 = e1.vup_id 
       AND de.identifier_value = e1.sha256
    LEFT JOIN derived.vector_email e2 
        ON de.vup_id_2 = e2.vup_id 
       AND de.identifier_value = e2.sha256
    WHERE de.identifier_type = 'hem_sha256'
)
SELECT 
    'HEM Duplicate Patterns' as analysis_type,
    CASE 
        WHEN vup1_email_type = vup2_email_type THEN 'Same Email Type (' || vup1_email_type || ')'
        ELSE 'Mixed Email Types (' || COALESCE(vup1_email_type, 'null') || ' vs ' || COALESCE(vup2_email_type, 'null') || ')'
    END as email_type_pattern,
    COUNT(*) as duplicate_pair_count,
    COUNT(DISTINCT hem_sha256) as unique_hems,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as pct_of_hem_duplicates
FROM hem_duplicates
GROUP BY 
    CASE 
        WHEN vup1_email_type = vup2_email_type THEN 'Same Email Type (' || vup1_email_type || ')'
        ELSE 'Mixed Email Types (' || COALESCE(vup1_email_type, 'null') || ' vs ' || COALESCE(vup2_email_type, 'null') || ')'
    END
ORDER BY duplicate_pair_count DESC;

-- Analyze email_type variety within duplicate groups containing HEMs
WITH hem_groups AS (
    SELECT DISTINCT
        vdg.group_id,
        vdg.vup_id
    FROM #vup_duplicate_groups vdg
    WHERE EXISTS (
        SELECT 1 
        FROM #duplicate_edges de 
        WHERE de.identifier_type = 'hem_sha256'
          AND vdg.vup_id IN (de.vup_id_1, de.vup_id_2)
    )
),
group_email_types AS (
    SELECT 
        hg.group_id,
        COUNT(DISTINCT ve.email_type) as email_type_count,
        LISTAGG(DISTINCT ve.email_type, ', ') WITHIN GROUP (ORDER BY ve.email_type) as email_types
    FROM hem_groups hg
    INNER JOIN derived.vector_email ve 
        ON hg.vup_id = ve.vup_id
    WHERE ve.sha256 IS NOT NULL
    GROUP BY hg.group_id
)
SELECT 
    'Email Type Variety in HEM Groups' as analysis_type,
    email_type_count as distinct_email_types,
    COUNT(*) as group_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as pct_of_hem_groups
FROM group_email_types
GROUP BY email_type_count
ORDER BY email_type_count;

-- ============================================================================
-- STEP 7: CROSS-IDENTIFIER DUPLICATION MATRIX
-- ============================================================================

-- Build matrix showing which identifier types co-occur in duplicate detection
-- Helps understand cross-source identifier reliability and overlap patterns
WITH identifier_pairs AS (
    SELECT 
        de1.identifier_type as id_type_1,
        de2.identifier_type as id_type_2,
        COUNT(*) as co_occurrence_count
    FROM #duplicate_edges de1
    INNER JOIN #duplicate_edges de2
        ON (de1.vup_id_1 = de2.vup_id_1 OR de1.vup_id_1 = de2.vup_id_2 
         OR de1.vup_id_2 = de2.vup_id_1 OR de1.vup_id_2 = de2.vup_id_2)
       AND de1.identifier_type <= de2.identifier_type  -- Prevent duplicate pairs
    GROUP BY de1.identifier_type, de2.identifier_type
)
SELECT 
    'Cross-Identifier Duplication Matrix' as analysis_type,
    id_type_1,
    id_type_2,
    co_occurrence_count,
    ROUND(100.0 * co_occurrence_count / SUM(co_occurrence_count) OVER(), 2) as pct_of_total
FROM identifier_pairs
ORDER BY co_occurrence_count DESC;

-- ============================================================================
-- STEP 8: SAMPLE LARGEST GROUPS FOR MANUAL REVIEW
-- ============================================================================

-- Display top 25 largest duplicate groups for manual inspection
-- Large groups often indicate systematic data quality issues needing investigation
WITH group_details AS (
    SELECT 
        group_id,
        COUNT(*) as group_size,
        LISTAGG(vup_id::VARCHAR, ', ') WITHIN GROUP (ORDER BY vup_id) as vup_ids_sample,
        MIN(vup_id) as min_vup_id
    FROM #vup_duplicate_groups
    GROUP BY group_id
)
SELECT 
    'Largest Duplicate Groups Sample' as sample_type,
    group_id,
    group_size,
    min_vup_id as suggested_winner_vup_id,
    CASE 
        WHEN group_size <= 5 THEN vup_ids_sample
        ELSE LEFT(vup_ids_sample, 100) || '... (truncated)'
    END as vup_ids_preview
FROM group_details
ORDER BY group_size DESC, group_id
LIMIT 25;

-- ============================================================================
-- STEP 9: FINAL SUMMARY AND EXECUTION REPORT
-- ============================================================================

-- Comprehensive final summary for implementation planning
WITH 
current_counts AS (
    SELECT 
        COUNT(DISTINCT vup_id) as total_persons_current
    FROM derived.vector_universal_person
),
duplicate_impact AS (
    SELECT 
        COUNT(DISTINCT group_id) as duplicate_groups,
        COUNT(*) as duplicate_vup_instances,
        COUNT(*) - COUNT(DISTINCT group_id) as vup_ids_to_eliminate
    FROM #vup_duplicate_groups
),
identifier_coverage AS (
    SELECT 
        SUM(CASE WHEN identifier_type = 'linkedin_url' THEN 1 ELSE 0 END) as linkedin_edges,
        SUM(CASE WHEN identifier_type = 'fbf_id' THEN 1 ELSE 0 END) as fbf_edges,
        SUM(CASE WHEN identifier_type = 'pdl_id' THEN 1 ELSE 0 END) as pdl_edges,
        SUM(CASE WHEN identifier_type = 'hem_sha256' THEN 1 ELSE 0 END) as hem_edges
    FROM #duplicate_edges
)
SELECT 
    'ENG-1914 Part 3: Duplicate Detection Complete' as execution_status,
    cc.total_persons_current as current_person_count,
    di.duplicate_groups as duplicate_groups_found,
    di.duplicate_vup_instances as vup_instances_in_groups,
    di.vup_ids_to_eliminate as vup_ids_to_consolidate,
    cc.total_persons_current - di.vup_ids_to_eliminate as projected_person_count_after_merge,
    ROUND(100.0 * di.vup_ids_to_eliminate / cc.total_persons_current, 2) as duplicate_rate_pct,
    ic.linkedin_edges as linkedin_duplicate_edges,
    ic.fbf_edges as fbf_duplicate_edges,
    ic.pdl_edges as pdl_duplicate_edges,
    ic.hem_edges as hem_duplicate_edges,
    GETDATE() as analysis_completed_at
FROM current_counts cc
CROSS JOIN duplicate_impact di
CROSS JOIN identifier_coverage ic;

-- ============================================================================
-- EXECUTION COMPLETE - TEMP TABLES READY FOR PART 4
-- ============================================================================
-- The following temporary tables are now available for Parts 4-5:
-- 
-- #duplicate_identifiers: All identifier pairs that indicate duplicates
--   Columns: identifier_type, vup_id_1, vup_id_2, identifier_value
--   Row count: All duplicate pairs across 4 identifier types
-- 
-- #duplicate_edges: Deduplicated graph edges for transitive closure
--   Columns: vup_id_1, vup_id_2, identifier_type, identifier_value
--   Row count: Unique edges in duplicate detection graph
-- 
-- #vup_duplicate_groups: Final group assignments after transitive closure
--   Columns: vup_id, group_id, max_iteration_reached
--   Row count: All vup_ids involved in duplicates with their group assignment
--   Usage: Winner selection (Part 4) uses MIN(vup_id) as winner per group_id
-- 
-- NEXT STEPS:
-- 1. Review analysis output above, especially largest groups
-- 2. Investigate any groups with size > 10 for data quality issues
-- 3. Confirm duplicate rate aligns with expectations (~1% of persons)
-- 4. Proceed to Part 4 (Winner Selection) only after review approval
-- ============================================================================
