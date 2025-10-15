-- ============================================================================-- ============================================================================

-- ENG-1973: Deployment Validation Suite (Production-Ready)-- ENG-1973 Part 11: Validation Suite for HEM Export

-- ============================================================================-- Execute: After creating base.v_email_hem_best view

-- All six validations must show PASS before DynamoDB export-- ============================================================================

-- Redshift-compatible: SUM(CASE...), null-safe comparisons, proper CTE scope

-- ============================================================================-- Validation 1: Exactly one row per HEM

SELECT

-- V1: HEM Uniqueness (Andrew's core requirement)  'Uniqueness: One Row Per HEM' AS validation_name,

SELECT  COUNT(*) AS total_rows,

  'V1: Uniqueness - One Row Per HEM' AS validation_name,  COUNT(DISTINCT hem) AS distinct_hems,

  COUNT(*) AS total_rows,  CASE 

  COUNT(DISTINCT hem) AS distinct_hems,    WHEN COUNT(*) = COUNT(DISTINCT hem) 

  COUNT(*) - COUNT(DISTINCT hem) AS duplicate_count,    THEN '✓ PASS - Export is unique by HEM'

  CASE     ELSE '✗ FAIL - Duplicate HEMs detected, investigate ranking logic'

    WHEN COUNT(*) = COUNT(DISTINCT hem)   END AS status

    THEN '✓ PASS - Export is unique by HEM'FROM base.v_email_hem_best;

    ELSE '✗ FAIL - Duplicates detected'

  END AS status-- Validation 2: Unresolved HEM coverage 

FROM derived.v_email_hem_best;SELECT

  'Coverage: Unresolved HEMs Included' AS validation_name,

-- V2: Unresolved HEM Coverage  COUNT(*) AS total_hems,

SELECT  COUNT(CASE WHEN vup_id IS NOT NULL THEN 1 END) AS resolved_hems,

  'V2: Coverage - Unresolved HEMs Included' AS validation_name,  COUNT(CASE WHEN vup_id IS NULL THEN 1 END) AS unresolved_hems,

  COUNT(*) AS total_hems,  ROUND(100.0 * COUNT(CASE WHEN vup_id IS NULL THEN 1 END) / NULLIF(COUNT(*), 0), 2) AS pct_unresolved,

  SUM(CASE WHEN vup_id IS NOT NULL THEN 1 ELSE 0 END) AS resolved_hems,  CASE 

  SUM(CASE WHEN vup_id IS NULL THEN 1 ELSE 0 END) AS unresolved_hems,    WHEN COUNT(CASE WHEN vup_id IS NULL THEN 1 END) > 0

  ROUND(100.0 * SUM(CASE WHEN vup_id IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS pct_resolved    THEN '✓ PASS - Unresolved HEMs included for cleartext resolution'

FROM derived.v_email_hem_best;    ELSE '⚠ WARNING - No unresolved HEMs found (verify if expected)'

  END AS status

-- V3: Determinism Test (null-safe comparison)FROM base.v_email_hem_best;

WITH contested_hems AS (

  SELECT hem -- Validation 3: Determinism test (same query produces same results)

  FROM derived.vector_emailWITH contested_hems AS (

  WHERE vup_id IS NOT NULL  -- Find HEMs claimed by multiple VUPs in source data

  GROUP BY hem   SELECT hem 

  HAVING COUNT(DISTINCT vup_id) > 1  FROM derived.vector_email_new

  LIMIT 200  WHERE vup_id IS NOT NULL

),  GROUP BY hem 

run1 AS (  HAVING COUNT(DISTINCT vup_id) > 1

  SELECT hem, vup_id   LIMIT 200

  FROM derived.v_email_hem_best ),

  WHERE hem IN (SELECT hem FROM contested_hems)run1 AS (

),  SELECT hem, vup_id 

run2 AS (  FROM base.v_email_hem_best 

  SELECT hem, vup_id   WHERE hem IN (SELECT hem FROM contested_hems)

  FROM derived.v_email_hem_best ),

  WHERE hem IN (SELECT hem FROM contested_hems)run2 AS (

)  SELECT hem, vup_id 

SELECT   FROM base.v_email_hem_best 

  'V3: Determinism - Consistency Check' AS validation_name,  WHERE hem IN (SELECT hem FROM contested_hems)

  COUNT(*) AS hems_tested,)

  SUM(CASE SELECT 

    WHEN (r1.vup_id = r2.vup_id) OR (r1.vup_id IS NULL AND r2.vup_id IS NULL)   'Determinism: Tie-Breaking Consistency' AS validation_name,

    THEN 1 ELSE 0   COUNT(*) AS hems_tested,

  END) AS matches,  SUM(CASE WHEN r1.vup_id = r2.vup_id THEN 1 ELSE 0 END) AS matches,

  ROUND(100.0 * SUM(CASE   SUM(CASE WHEN r1.vup_id IS DISTINCT FROM r2.vup_id THEN 1 ELSE 0 END) AS mismatches,

    WHEN (r1.vup_id = r2.vup_id) OR (r1.vup_id IS NULL AND r2.vup_id IS NULL)   CASE 

    THEN 1 ELSE 0     WHEN SUM(CASE WHEN r1.vup_id IS DISTINCT FROM r2.vup_id THEN 1 END) = 0

  END) / NULLIF(COUNT(*), 0), 2) AS match_percentage,    THEN '✓ PASS - Tie-breaking is deterministic'

  CASE     ELSE '✗ FAIL - Non-deterministic results detected, investigate ORDER BY'

    WHEN SUM(CASE   END AS status

      WHEN NOT ((r1.vup_id = r2.vup_id) OR (r1.vup_id IS NULL AND r2.vup_id IS NULL)) FROM run1 r1 

      THEN 1 ELSE 0 INNER JOIN run2 r2 USING (hem);

    END) = 0

    THEN '✓ PASS - Deterministic'-- Validation 4: Coverage parity with source table

    ELSE '✗ FAIL - Non-deterministic'SELECT

  END AS status  'Coverage: Source Table Parity' AS validation_name,

FROM run1 r1   (SELECT COUNT(DISTINCT hem) FROM derived.vector_email_new) AS source_distinct_hems,

INNER JOIN run2 r2 ON r1.hem = r2.hem;  (SELECT COUNT(*) FROM base.v_email_hem_best) AS export_total_hems,

  (SELECT COUNT(DISTINCT hem) FROM derived.vector_email_new) - 

-- V4: Source Table Coverage Parity    (SELECT COUNT(*) FROM base.v_email_hem_best) AS missing_hems,

WITH source_metrics AS (  CASE 

  SELECT COUNT(DISTINCT sha256) AS source_distinct_hems    WHEN (SELECT COUNT(DISTINCT hem) FROM derived.vector_email_new) = 

  FROM derived.vector_email         (SELECT COUNT(*) FROM base.v_email_hem_best)

  WHERE sha256 IS NOT NULL    THEN '✓ PASS - All source HEMs present in export'

),    ELSE '✗ FAIL - Some HEMs missing, investigate view logic'

export_metrics AS (  END AS status;

  SELECT COUNT(*) AS export_total_hems

  FROM derived.v_email_hem_best-- Validation 5: Cleartext preference safety

)-- If any source row for a HEM has cleartext email, the selected row must also have it

SELECTWITH hems_with_cleartext AS (

  'V4: Coverage Parity' AS validation_name,  SELECT hem

  sm.source_distinct_hems,  FROM derived.vector_email_new

  em.export_total_hems,  GROUP BY hem

  sm.source_distinct_hems - em.export_total_hems AS missing_hems,  HAVING SUM(CASE WHEN email IS NOT NULL AND TRIM(email) != '' THEN 1 ELSE 0 END) > 0

  CASE )

    WHEN sm.source_distinct_hems = em.export_total_hemsSELECT 

    THEN '✓ PASS - 100% coverage'  'Cleartext: Preference Enforcement' AS validation_name,

    ELSE '✗ FAIL - Missing HEMs'  COUNT(*) AS hems_with_cleartext_in_source,

  END AS status  COUNT(*) FILTER (WHERE v.email IS NULL OR TRIM(v.email) = '') AS selected_without_cleartext,

FROM source_metrics sm, export_metrics em;  CASE 

    WHEN COUNT(*) FILTER (WHERE v.email IS NULL OR TRIM(v.email) = '') = 0

-- V5: Cleartext Preference Enforcement    THEN '✓ PASS - Selected rows preserve cleartext when available'

WITH hems_with_cleartext AS (    ELSE '✗ FAIL - Some selections lost cleartext email, check ranking'

  SELECT sha256 AS hem  END AS status

  FROM derived.vector_emailFROM base.v_email_hem_best v

  WHERE sha256 IS NOT NULLINNER JOIN hems_with_cleartext h USING (hem);

  GROUP BY sha256

  HAVING SUM(CASE WHEN email IS NOT NULL AND TRIM(email) <> '' THEN 1 ELSE 0 END) > 0-- Sample tie-breaking decisions for manual review

)WITH multi_vup_hems AS (

SELECT   SELECT 

  'V5: Cleartext Preference' AS validation_name,    hem,

  COUNT(*) AS hems_with_cleartext_available,    COUNT(DISTINCT vup_id) AS vup_candidate_count,

  SUM(CASE WHEN v.email IS NULL OR TRIM(v.email) = '' THEN 1 ELSE 0 END) AS selected_without_cleartext,    LISTAGG(DISTINCT vup_id::VARCHAR, ', ') WITHIN GROUP (ORDER BY vup_id) AS all_candidate_vups

  ROUND(100.0 * SUM(CASE WHEN v.email IS NOT NULL AND TRIM(v.email) <> '' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS cleartext_preservation_rate,  FROM derived.vector_email_new

  CASE   WHERE vup_id IS NOT NULL

    WHEN SUM(CASE WHEN v.email IS NULL OR TRIM(v.email) = '' THEN 1 ELSE 0 END) = 0  GROUP BY hem

    THEN '✓ PASS - 100% preservation'  HAVING COUNT(DISTINCT vup_id) > 1

    ELSE '✗ FAIL - Lost cleartext'  LIMIT 20

  END AS status)

FROM derived.v_email_hem_best vSELECT 

INNER JOIN hems_with_cleartext h ON v.hem = h.hem;  'Sample Tie-Breaking Decisions' AS review_type,

  mvh.hem,

-- V6: Duplicate Audit (comprehensive check)  mvh.vup_candidate_count AS competing_vups,

WITH duplicate_check AS (  mvh.all_candidate_vups AS candidates,

  SELECT   export.vup_id AS chosen_vup,

    hem,  export.email,

    COUNT(*) AS occurrence_count  'Review: Chosen VUP should have highest job count among candidates' AS expectation

  FROM derived.v_email_hem_bestFROM multi_vup_hems mvh

  GROUP BY hemINNER JOIN base.v_email_hem_best export ON mvh.hem = export.hem

  HAVING COUNT(*) > 1ORDER BY mvh.vup_candidate_count DESC;

)

SELECT -- Export readiness summary

  'V6: Duplicate Audit' AS validation_name,SELECT 

  COUNT(*) AS duplicate_hems_found,  '=== EXPORT READINESS SUMMARY ===' AS checkpoint,

  CASE   (SELECT COUNT(*) FROM base.v_email_hem_best) AS total_hems_for_export,

    WHEN COUNT(*) = 0  (SELECT COUNT(*) FROM base.v_email_hem_best WHERE vup_id IS NOT NULL) AS resolved_hems,

    THEN '✓ PASS - No duplicates'  (SELECT COUNT(*) FROM base.v_email_hem_best WHERE vup_id IS NULL) AS unresolved_hems,

    ELSE '✗ FAIL - Duplicates exist'  'All five validations must show PASS status before DynamoDB export' AS requirement,

  END AS status  'Review sample tie-breaking decisions to confirm ranking logic correctness' AS manual_check;

FROM duplicate_check;

-- Record validation completion in metadata

-- CRITICAL FIX: CTE redefined here as scope terminated at prior queryINSERT INTO derived.vector_email_migration_metadata_eng1973 

WITH duplicate_check AS (    (migration_step, records_affected, notes)

  SELECT VALUES 

    hem,    ('hem_export_validation', 

    COUNT(*) AS occurrence_count     (SELECT COUNT(*) FROM base.v_email_hem_best),

  FROM derived.v_email_hem_best     'Completed 5 validation checks for base.v_email_hem_best: uniqueness, coverage, determinism, parity, cleartext preference');

  GROUP BY hem

  HAVING COUNT(*) > 1-- Display all migration steps

)SELECT * FROM derived.vector_email_migration_metadata_eng1973

SELECT ORDER BY execution_timestamp;

  'Duplicate Examples (if any):' AS diagnostic_section,

  hem,
  occurrence_count
FROM duplicate_check
ORDER BY occurrence_count DESC
LIMIT 10;

-- Sample tie-breaking decisions (LISTAGG lexicographic ordering)
-- NOTE: LISTAGG uses lexicographic ordering by default which approximates
-- lowest ID selection for validation purposes
WITH base_candidates AS (
  SELECT DISTINCT hem, vup_id
  FROM derived.vector_email
  WHERE vup_id IS NOT NULL
),
multi_vup_hems AS (
  SELECT 
    hem,
    COUNT(*) AS vup_candidate_count,
    LISTAGG(vup_id, ', ') WITHIN GROUP (ORDER BY vup_id) AS all_candidate_vups
  FROM base_candidates
  GROUP BY hem
  HAVING COUNT(*) > 1
  ORDER BY vup_candidate_count DESC
  LIMIT 20
)
SELECT 
  '====== SAMPLE TIE-BREAKING DECISIONS ======' AS section_header,
  mvh.hem,
  mvh.vup_candidate_count AS competing_vups_count,
  mvh.all_candidate_vups AS all_candidates,
  export.vup_id AS chosen_vup,
  CASE 
    WHEN export.vup_id = SPLIT_PART(mvh.all_candidate_vups, ', ', 1) 
    THEN 'Lowest ID (lexicographic approximation)'
    ELSE 'Jobs/recency criteria'
  END AS selection_reasoning
FROM multi_vup_hems mvh
INNER JOIN derived.v_email_hem_best export ON mvh.hem = export.hem
ORDER BY mvh.vup_candidate_count DESC;

-- Deployment readiness summary
SELECT 
  '========== DEPLOYMENT READINESS ==========' AS summary_header,
  (SELECT COUNT(*) FROM derived.v_email_hem_best) AS total_hems_for_export,
  (SELECT COUNT(*) FROM derived.v_email_hem_best WHERE vup_id IS NOT NULL) AS resolved_hems,
  (SELECT COUNT(*) FROM derived.v_email_hem_best WHERE vup_id IS NULL) AS unresolved_hems,
  'All six validations must show PASS before DynamoDB export' AS requirement;
