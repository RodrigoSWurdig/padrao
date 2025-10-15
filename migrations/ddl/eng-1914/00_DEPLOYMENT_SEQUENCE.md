# ENG-1914 Deployment Sequence Requirements

## Critical Sequencing Constraint

Part 5 cleanup (05_cleanup_columns.sql) MUST execute only after cube schema changes deploy to production. Executing cleanup before cube deployment will break active queries that reference the old identifier columns (linkedin_url, fbf_id, pdl_id).

## Required Execution Order

### Phase 1: Foundation Setup
1. Execute `00_create_metadata_table.sql`
2. Execute `00_pre_migration_cleanup.sql`
3. Execute `01_backup_and_create_tables.sql`
4. Execute `02_migrate_data.sql`

### Phase 2: Duplicate Processing
5. Execute `03_identify_duplicates.sql`
6. Execute `03b_label_propagation_iteration.sql` repeatedly until convergence
7. Execute `03c_finalize_duplicate_groups.sql`
8. Execute `04_merge_duplicates.sql`

### Phase 3: Cube Deployment (Must Complete Before Phase 4)
9. Deploy updated cube schemas to production
10. Verify cube queries execute successfully
11. Confirm no active queries reference old identifier columns

### Phase 4: Cleanup (Execute Only After Cube Deployment Verified)
12. Execute `05_cleanup_columns.sql`

## Pre-Flight Validation

Before executing Phase 4, run this validation query:

```sql
SELECT 
  query,
  starttime,
  querytxt
FROM stl_query
WHERE querytxt ILIKE '%vector_universal_person%'
  AND (querytxt ILIKE '%linkedin_url%' OR querytxt ILIKE '%fbf_id%' OR querytxt ILIKE '%pdl_id%')
  AND starttime > DATEADD(hour, -24, GETDATE())
ORDER BY starttime DESC
LIMIT 20;
```

If this returns results showing active usage, delay Phase 4 until those queries migrate to junction table patterns.

## Rollback Procedure

If issues are detected after any phase:

1. **Before Phase 2 (merge)**: Simply restore from backup tables
2. **After Phase 2**: Execute `99_rollback.sql` to restore merged persons
3. **After Phase 4 (cleanup)**: Cannot rollback column drops - use backup table restore

## Monitoring

Monitor these metrics after each phase:

- **Phase 1**: Junction table row counts match source tables
- **Phase 2**: Active person count reduction percentage (expect 10-30%)
- **Phase 3**: Cube query success rate remains 100%
- **Phase 4**: No query errors referencing old columns

## Communication Plan

1. **Before Phase 2**: Notify data consumers of potential person ID changes
2. **Before Phase 3**: Coordinate cube deployment with DevOps
3. **After Phase 3**: Wait minimum 2 weeks before Phase 4
4. **Before Phase 4**: Send final notice about deprecated column removal

---

**Document Version**: 1.0  
**Last Updated**: 2025-10-15  
**Owner**: Data Engineering Team  
**Related Tickets**: ENG-1914
