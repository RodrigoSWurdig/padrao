-- ============================================================================
-- ENG-1914: Label Propagation Iteration (Manual Execution)
-- ============================================================================
-- Execute repeatedly until changes_detected = 0
-- Typical convergence: 8-12 iterations
-- ============================================================================

-- Initialize labels on first execution
CREATE TEMP TABLE IF NOT EXISTS #labels AS
SELECT vup_id AS node, vup_id AS label
FROM (
  SELECT u AS vup_id FROM #edges
  UNION
  SELECT v AS vup_id FROM #edges
) t;

-- Propagate minimum neighbor label
DROP TABLE IF EXISTS #labels_next;

CREATE TEMP TABLE #labels_next AS
SELECT 
  l.node,
  LEAST(l.label, COALESCE(m.min_neighbor_label, l.label)) AS label
FROM #labels l
LEFT JOIN (
  SELECT e.v AS node, MIN(l2.label) AS min_neighbor_label
  FROM #edges e 
  JOIN #labels l2 ON e.u = l2.node
  GROUP BY e.v
  UNION ALL
  SELECT e.u AS node, MIN(l2.label) AS min_neighbor_label
  FROM #edges e 
  JOIN #labels l2 ON e.v = l2.node
  GROUP BY e.u
) m ON l.node = m.node;

-- Check convergence
SELECT 
  'Iteration Status' AS status,
  COUNT(*) AS changes_detected,
  CASE 
    WHEN COUNT(*) = 0 THEN 'CONVERGED - Execute finalization script'
    ELSE 'CONTINUE - Run this script again'
  END AS next_action
FROM #labels l
JOIN #labels_next n ON l.node = n.node
WHERE l.label <> n.label;

-- Swap for next iteration
DROP TABLE IF EXISTS #labels_old;
ALTER TABLE #labels RENAME TO #labels_old;
ALTER TABLE #labels_next RENAME TO #labels;
DROP TABLE IF EXISTS #labels_old;
