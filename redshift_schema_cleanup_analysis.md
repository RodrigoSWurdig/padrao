# Redshift Schema Cleanup Analysis

## Overview

This document provides a comprehensive analysis of Redshift schema objects that can be safely deleted based on Cube configuration usage and view dependencies.

## Analysis Methodology

1. **Cube Configuration Analysis**: Examined all Cube model files to identify directly referenced tables/views
2. **View Dependency Analysis**: Analyzed all view definitions to identify indirect dependencies
3. **Application Usage Analysis**: Checked vector-api codebase for direct table references
4. **Zero-ETL Replication Analysis**: Identified views that replicate data from RDS instances (vectordb)

## 🟢 DEFINITELY SAFE TO DELETE (100% Certain)

These objects have **NO references** in:

- Cube configurations
- View definitions
- Materialized view definitions
- Application code

### Backup/Old Tables ✅ DELETED

- ~~`dev.derived.vector_universal_job_old`~~ ✅ **DELETED**
- ~~`dev.derived.vector_universal_person_backup`~~ ✅ **DELETED**

### Unused Base Views (NOT used by temp views) ✅ DELETED

- ~~`dev.base.segment_membership_buyer_events_with_vup`~~ ✅ **DELETED**

### Unused Derived Views ✅ DELETED

- ~~`dev.derived.market_pulse`~~ ✅ **DELETED**
- ~~`dev.derived.market_pulse_custom_topics`~~ ✅ **DELETED**
- ~~`dev.derived.market_pulse_custom_topics_taxonomy`~~ ✅ **DELETED**
- ~~`dev.derived.market_pulse_taxonomy`~~ ✅ **DELETED**

## 🟡 TEMPORARY VIEWS - RESTORED (Used by ETL Processing)

**Status**: These views have been restored and are now actively used by ETL processes. The `fbf_*` views are also preserved for future development.

### Temporary Views (ETL Processing)

- 🟡 `dev.derived.temp_vector_emails` (depends on: fbf_full_email_list, live_intent, fbf_up_to_hem)
- 🟡 `dev.derived.temp_vector_maids` (depends on: fbf_device_360)
- 🟡 `dev.derived.temp_vector_phones` (depends on: fbf_person, fbf_companies)
- 🟡 `dev.derived.temp_vector_universal_company` (depends on: pdl_companies, fbf_companies)
- 🟡 `dev.derived.temp_vector_universal_job` (depends on: fbf_jobs, pdl_jobs)
- 🟡 `dev.derived.temp_vector_universal_person` (depends on: pdl_person, fbf_person)

### Base Views Required by Temp Views (Also Preserved for Future Development)

- 🟡 `dev.base.fbf_companies` (used by temp_vector_phones, temp_vector_universal_company + future development)
- 🟡 `dev.base.fbf_device_360` (used by temp_vector_maids + future development)
- 🟡 `dev.base.fbf_full_email_list` (used by temp_vector_emails + future development)
- 🟡 `dev.base.fbf_jobs` (used by temp_vector_universal_job + future development)
- 🟡 `dev.base.fbf_person` (used by temp_vector_phones, temp_vector_universal_person + future development)
- 🟡 `dev.base.fbf_up_to_hem` (used by temp_vector_emails + future development)
- 🟡 `dev.base.live_intent` (used by temp_vector_emails)
- 🟡 `dev.base.pdl_companies` (used by temp_vector_universal_company)
- 🟡 `dev.base.pdl_jobs` (used by temp_vector_universal_job)
- 🟡 `dev.base.pdl_person` (used by temp_vector_universal_person)

## 🔴 CRITICAL DEPENDENCIES - DO NOT DELETE

### Core Tables (Used by Cube + Application)

- 🔴 `dev.derived.vector_email` - Used by `emails` cube + vector-api writes
- 🔴 `dev.derived.vector_maid` - Used by `maids` cube + vector-api writes
- 🔴 `dev.derived.vector_phone` - Used by `phones` cube
- 🔴 `dev.derived.vector_universal_company` - Used by `universal_company` cube + views
- 🔴 `dev.derived.vector_universal_job` - Used by `universal_job` cube + views
- 🔴 `dev.derived.vector_universal_person` - Used by `universal_person` cube + views + vector-api writes

### Views Used by Cube Configurations

- 🔴 `dev.base.market_pulse_2_vup_20250825` - Used by `market_pulse_2_vup_20250825` cube
- 🔴 `dev.base.mv_market_pulse_taxonomy` - Used by `market_pulse_taxonomy` cube

### Views with Complex Dependencies

- 🔴 `dev.derived.mv_fbf_to_vup` - Used by materialized views
- 🔴 `dev.base.mv_market_pulse_2_20250825` - Used by market_pulse_2_vup_20250825
- 🔴 `dev.base.business_units` - Used by zero-ETL replication + various views
- 🔴 `dev.base.account_list_memberships` - Used by zero-ETL replication + various views
- 🔴 `dev.base.visitor_activities` - Used by various views
- 🔴 `dev.base.visitors` - Used by zero-ETL replication + various views

### Zero-ETL RDS Replications (CRITICAL - DO NOT DELETE)

- 🔴 `dev.base.visitors` - Materialized view from `vectordb.public.visitors`
- 🔴 `dev.base.segments` - Auto-refresh materialized view from `vectordb.public.segments`
- 🔴 `dev.base.segment_events` - Auto-refresh materialized view from `vectordb.public.segment_events`
- 🔴 `dev.base.business_units` - Auto-refresh materialized view from `vectordb.public.business_units`
- 🔴 `dev.base.account_lists` - Auto-refresh materialized view from `vectordb.public.account_lists`
- 🔴 `dev.base.account_list_memberships` - Auto-refresh materialized view from `vectordb.public.account_list_memberships`

## ✅ FUNNEL VISION CLEANUP - COMPLETED

**Status**: All funnel vision views and their dependencies have been successfully removed.

### Funnel Vision Views ✅ DELETED

- 🟢 `dev.derived.funnel_vision` - **DELETED**
- 🟢 `dev.derived.funnel_vision_cube` - **DELETED**
- 🟢 `dev.derived.funnel_vision_cube_new` - **DELETED**

### Views Cleaned Up as Result ✅ DELETED

- 🟢 `dev.derived.buyer_events` - **DELETED** (only used by funnel vision views + non-functional dashboard)
- 🟢 `dev.base.buyer_events_visitors_20250611` - **DELETED** (only used by buyer_events)
- 🟢 `dev.base.buyer_events_market_pulse_2_20250601` - **DELETED** (only used by buyer_events)
- 🟢 `dev.base.buyer_events_visitors` - **DELETED** (unused)
- 🟢 `dev.base.buyer_events_market_pulse` - **DELETED** (unused)

## ✅ MARKET PULSE CLEANUP - COMPLETED

### Market Pulse Views ✅ DELETED

- 🟢 `dev.base.mv_market_pulse` - **DELETED** (old materialized view, only used by market_pulse_new)
- 🟢 `dev.base.market_pulse` - **DELETED** (old complex materialized view)
- 🟢 `dev.base.market_pulse_new` - **DELETED** (view depending on old mv_market_pulse)
- 🟢 `dev.base.market_pulse_2` - **DELETED** (view depending on mv_market_pulse_2_new)
- 🟢 `dev.base.market_pulse_2_vup` - **DELETED** (view referencing missing mv_market_pulse_2_20250715)
- 🟢 `dev.base.mv_market_pulse_2_new` - **DELETED** (unused materialized view)

### Market Pulse Views 🔴 PRESERVED

- 🔴 `dev.base.mv_market_pulse_2_20250825` - **ACTIVE** (used by market_pulse_2_vup_20250825)
- 🔴 `dev.base.market_pulse_2_vup_20250825` - **ACTIVE** (referenced in Cube configs)
- 🔴 `dev.base.mv_market_pulse_taxonomy` - **ACTIVE** (referenced in Cube configs)
- 🔴 `dev.base.mv_market_pulse_vup` - **PRESERVED** (for future development)

## Recommended Deletion Order

### Phase 1: Definitely Safe (🟢)

Start with objects that have zero dependencies:

1. Backup tables (`*_old`, `*_backup`)
2. Temporary views (`temp_*`)
3. Unused base views (fbf*\*, pdl*\_, segment\_\_)
4. Unused derived views (market_pulse variants)

### Phase 2: Verification Required (🟡)

After Phase 1, investigate:

1. Historical/versioned views
2. Legacy funnel vision views

### Phase 3: Never Delete (🔴)

Keep all objects in the critical dependencies list.

## Pre-Deletion Verification

Before deleting any object, run these queries:

```sql
-- Check for view dependencies
SELECT DISTINCT
    schemaname,
    viewname,
    definition
FROM pg_views
WHERE definition LIKE '%object_name_you_want_to_delete%';

-- Check for materialized view dependencies
SELECT DISTINCT
    schemaname,
    matviewname,
    definition
FROM pg_matviews
WHERE definition LIKE '%object_name_you_want_to_delete%';

-- Check for table dependencies
SELECT DISTINCT
    schemaname,
    tablename
FROM pg_tables
WHERE tablename LIKE '%object_name_you_want_to_delete%';
```

## Cube Configuration References

### Directly Referenced in Cube Configs:

- `dev.derived.vector_email` → `emails` cube
- `dev.derived.vector_maid` → `maids` cube
- `dev.derived.vector_phone` → `phones` cube
- `dev.derived.vector_universal_company` → `universal_company` cube
- `dev.derived.vector_universal_job` → `universal_job` cube
- `dev.derived.vector_universal_person` → `universal_person` cube
- `dev.base.market_pulse_2_vup_20250825` → `market_pulse_2_vup_20250825` cube
- `dev.base.mv_market_pulse_taxonomy` → `market_pulse_taxonomy` cube

## Application Usage

### Direct Table Writes in vector-api:

- `derived.vector_universal_person` - Bulk insert operations
- `derived.vector_maid` - Bulk insert operations
- `derived.vector_email` - Bulk insert operations

## Summary

**Total Objects Analyzed**: ~60+ views/tables
**✅ Successfully Deleted**: 19 objects (100% of safe-to-delete objects + funnel vision + market*pulse cleanup)
**🔄 Restored for Future Development**: 7 objects (fbf*\* views + mv_market_pulse_vup)
**🟡 Temporary Views**: 16 objects (6 temp views + 10 base views they depend on) - **PRESERVED**
**🔴 Critical Dependencies**: 12+ objects (including 6 zero-ETL replications) - **PRESERVED**

**✅ CLEANUP COMPLETED**: All identified safe-to-delete schema objects have been successfully removed from the repository, including a complete cleanup of the funnel vision system, buyer_events dependencies, and unused market_pulse views.

**Successfully Deleted Objects (19 total):**

- 2 backup/old tables ✅ **DELETED**
- 1 unused base view (not used by temp views) ✅ **DELETED**
- 4 unused derived views ✅ **DELETED**
- 3 funnel vision views ✅ **DELETED**
- 5 buyer_events views (cleaned up as result) ✅ **DELETED**
- 6 market_pulse views (unused/duplicated) ✅ **DELETED**

**Restored for Future Development (7 total):**

- 1 market_pulse view (mv_market_pulse_vup) ✅ **RESTORED** (preserved for future development)
- 6 fbf\_\* views ✅ **RESTORED** (preserved for future development)

**Note**: The `buyer_events` schema was referenced by the non-functional dashboard service, but since the dashboard is considered irrelevant, these references did not prevent safe deletion.

**🔒 PRESERVED**: All critical dependencies remain intact, including:

- 6 zero-ETL replications from the `vectordb` RDS instance
- 6 temporary ETL views and their 10 base view dependencies
- All views with complex dependencies in the buyer_events/funnel_vision chain
