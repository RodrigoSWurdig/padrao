-- ============================================================================
-- ENG-1914: Finalize After Convergence
-- ============================================================================

CREATE TABLE #vup_duplicate_groups AS
SELECT label AS group_id, node AS vup_id
FROM #labels;

INSERT INTO derived.eng1914_migration_metadata
(migration_step, records_affected, status, notes)
VALUES (
  'connected_components_complete',
  (SELECT COUNT(*) FROM #vup_duplicate_groups),
  'success',
  'Label propagation converged'
);

-- Component size distribution
SELECT
  component_size,
  COUNT(*) AS component_count
FROM (
  SELECT group_id, COUNT(*) AS component_size
  FROM #vup_duplicate_groups
  GROUP BY group_id
) sizes
GROUP BY component_size
ORDER BY component_size;
