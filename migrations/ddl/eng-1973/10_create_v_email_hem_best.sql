-- ============================================================================
-- ENG-1973: HEM-Centric Export View (Production-Ready)
-- ============================================================================
-- CRITICAL: PARTITION BY hem (not vup_id) per Andrew requirement
-- 
-- Andrew feedback addressed: "This would only export one row per vup (as the 
-- canonical email), which will miss HEMs -- we need all of the HEMs for our 
-- resolution. This should be a row per hem instead"
--
-- ALL MUST-FIX ITEMS INCORPORATED:
-- 1. Cleartext preference ranking (Priority 2)
-- 2. Safe vup_id casting using REGEXP_INSTR
-- 3. TIMESTAMP '1970-01-01 00:00:00' instead of TIMESTAMP 'epoch'
-- 4. WITH NO SCHEMA BINDING at correct position
-- 5. CASE statement for null ordering (no NULLS LAST)
-- 6. SUM(CASE...) instead of FILTER(WHERE...)
-- 7. Null-safe comparisons without IS DISTINCT FROM
-- 8. Late binding for schema evolution
-- ============================================================================

DROP VIEW IF EXISTS derived.v_email_hem_best CASCADE;

CREATE VIEW derived.v_email_hem_best AS
WITH job_statistics AS (
  SELECT 
    vup_id, 
    COUNT(*) AS job_count
  FROM derived.vector_universal_job
  GROUP BY vup_id
),
person_activity AS (
  SELECT 
    vup_id,
    created_at,
    updated_at,
    GREATEST(COALESCE(updated_at, created_at), 
             COALESCE(created_at, updated_at)) AS most_recent_activity
  FROM derived.vector_universal_person
  WHERE merged_into_vup_id IS NULL
),
ranked_selections AS (
  SELECT 
    ve.sha256 AS hem,
    ve.email,
    ve.vup_id,
    CASE 
      WHEN ve.email LIKE '%@%'
      THEN LOWER(TRIM(SPLIT_PART(ve.email, '@', 2)))
      ELSE NULL
    END AS domain,
    ve.source,
    COALESCE(js.job_count, 0) AS job_count,
    pa.created_at AS vup_created_at,
    pa.most_recent_activity,
    ROW_NUMBER() OVER (
      PARTITION BY ve.sha256  -- CRITICAL: Partition by HEM not VUP
      ORDER BY
        -- Priority 1: Prefer resolved over unresolved
        CASE WHEN ve.vup_id IS NOT NULL THEN 0 ELSE 1 END,
        
        -- Priority 2: Prefer cleartext over hash-only (MUST-FIX addition)
        CASE WHEN ve.email IS NOT NULL AND TRIM(ve.email) <> '' THEN 0 ELSE 1 END,
        
        -- Priority 3: Most jobs wins
        COALESCE(js.job_count, 0) DESC,
        
        -- Priority 4: Most recent activity wins
        CASE 
          WHEN pa.most_recent_activity IS NULL THEN 0
          ELSE 1
        END DESC,
        pa.most_recent_activity DESC,
        
        -- Priority 5: Temporal tiebreaker with safe numeric casting
        CASE 
          WHEN REGEXP_INSTR(ve.vup_id, '^[0-9]+$') = 1 
          THEN CAST(ve.vup_id AS BIGINT)
          ELSE 9223372036854775807
        END ASC
    ) AS hem_rank
  FROM derived.vector_email ve
  LEFT JOIN person_activity pa ON ve.vup_id = pa.vup_id
  LEFT JOIN job_statistics js ON ve.vup_id = js.vup_id
  WHERE ve.sha256 IS NOT NULL
)
SELECT 
  hem,
  email,
  vup_id,
  domain,
  source,
  job_count,
  vup_created_at,
  most_recent_activity,
  TIMESTAMP '1970-01-01 00:00:00' AS last_verified
FROM ranked_selections
WHERE hem_rank = 1
WITH NO SCHEMA BINDING;

COMMENT ON VIEW derived.v_email_hem_best IS
'ENG-1973: HEM-centric export guaranteeing one row per HEM with deterministic VUP selection. Includes unresolved HEMs for cleartext lookup per Andrew feedback. PARTITION BY hem ensures all email hashes available for resolution workflows.';

INSERT INTO derived.eng1973_migration_metadata
(migration_step, records_affected, status, notes, created_at)
VALUES (
  'hem_export_view_created', 
  (SELECT COUNT(*) FROM derived.v_email_hem_best),
  'success',
  'Created HEM-centric view with cleartext preference and deterministic tie-breaking',
  CURRENT_TIMESTAMP
);
