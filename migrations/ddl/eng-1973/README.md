# ENG-1973: Vector Email Schema Enhancement

**Branch:** rwurdig/upd-vector-email  


---

## Quick Start

### 1. Pre-Migration Review

Review baseline metrics:
```
📄 00_production_schema_verification.md
```

### 2. Execution Order

Execute SQL files in numerical order:

```
01_analyze_current_data_quality.sql      - Identify data quality issues
02_create_backup_tables.sql              - Backup current table
03_create_new_schema.sql                 - Create new table structure
04_create_domains_table.sql              - Create reference table
05_migrate_data_to_new_schema.sql        - Migrate with deduplication
06_data_quality_validation.sql           - Validate migration
07_update_dependent_views.sql            - Update views
08_create_dynamodb_export_view.sql       - Create canonical view
09_verify_access_patterns.sql            - Test performance
10_create_v_email_hem_best.sql           - Create HEM-centric export view (Andrew's spec)
11_validate_v_email_hem_best.sql         - Validate HEM export view
12_dynamodb_table_and_loader.md          - DynamoDB infrastructure & pipeline docs
99_rollback.sql                          - IF NEEDED: Rollback
```

---

## Problem Overview

### Current Issues

1. **HEMs with Multiple VUPs:** Same email hash associated with different people (canonical VUP ambiguity)
2. **Mixed Cleartext/Hash-Only:** Same HEM has both cleartext and hash-only records (export ambiguity)
3. **No Deliverability Tracking:** Cannot determine which business email is most reliable
4. **No Domain Classification:** Cannot distinguish personal vs corporate emails

### Solution

- **UNIQUE Constraint:** Enforce `(hem, vup_id)` uniqueness
- **Priority-Based Deduplication:** cleartext > hash-only, business > personal, 5x5 > PDL
- **Domain Classification:** Reference table for personal/spam/company domains
- **Canonical Selection:** DynamoDB export view with best email per VUP

---

## Key Changes

### Schema Changes

| Change | Description |
|--------|-------------|
| Column Rename | `sha256` → `hem` (more descriptive) |
| New Columns | `domain`, `last_verified`, `created_at`, `updated_at` |
| New Constraint | `UNIQUE(hem, vup_id)` |
| Distribution | `DISTKEY(hem)` for fast HEM lookups |
| Sort Keys | `SORTKEY(vup_id, hem)` for person queries |

### New Objects

| Object | Purpose |
|--------|---------|
| `vector_email_new` | Enhanced schema with UNIQUE constraint |
| `vector_email_domains` | Reference table for domain classification |
| `v_dynamodb_email_export` | Canonical email view (one per VUP) |

---

## Validation Checklist

### ✅ Must Pass (All 5)

1. **UNIQUE Constraint:** Zero `(hem, vup_id)` duplicates
2. **Cleartext Preservation:** All original cleartext HEMs preserved
3. **Domain Extraction:** 100% valid domains for cleartext emails
4. **Required Fields:** 100% have `hem` and `data_source`
5. **HEM Format:** 100% are valid SHA256 hashes (64 hex chars)

### ⚠️ Expected Results

- Deduplication: 15-30% of records removed (expected)
- Cleartext coverage: ≥80% of records
- Business emails: 60-70% of records

---

## Rollback Instructions

If migration fails or issues detected:

```sql
-- Execute rollback script
\i 99_rollback.sql
```

**Rollback restores:**
- Original `vector_email` table from backup
- Original `temp_vector_emails` view
- All original data (zero data loss)

**Time:** 5-10 minutes

---

## Dependencies

### ⚠️ Critical Dependency

**ENG-1914 Part 4 must be completed first:**
- Part 4 performs HEM deduplication to ensure `(vup_id, sha256)` uniqueness
- Without this, ENG-1973 migration will fail UNIQUE constraint
- See: `migrations/ddl/eng-1914/PART_4_SUMMARY.md`



## File Descriptions

### Documentation

- **00_production_schema_verification.md** - Baseline metrics and approval checklist
- **ENG-1973_SUMMARY.md** - Comprehensive implementation guide (full details)
- **README.md** - This file (quick reference)

### Migration Scripts

- **01_analyze_current_data_quality.sql**
  - 7 data quality analyses
  - Identifies HEMs with multiple VUPs
  - Identifies mixed cleartext/hash-only records
  - Calculates expected deduplication impact

- **02_create_backup_tables.sql**
  - Creates `vector_email_backup_eng1973`
  - Creates migration metadata tracking table
  - Verifies backup completeness

- **03_create_new_schema.sql**
  - Creates `vector_email_new` with UNIQUE constraint
  - Sets DISTKEY and SORTKEY for performance
  - Adds new columns (domain, last_verified, timestamps)

- **04_create_domains_table.sql**
  - Creates `vector_email_domains` reference table
  - Populates company domains from `vector_universal_company`
  - Populates 75+ personal email providers
  - Populates 20+ spam/disposable domains

- **05_migrate_data_to_new_schema.sql**
  - Phase 1: Migrates (hem, vup_id) pairs with deduplication
  - Phase 2: Migrates hem-only records
  - Applies 5-tier priority ranking
  - Extracts domain from cleartext emails

- **06_data_quality_validation.sql**
  - Validation 1: UNIQUE constraint enforcement
  - Validation 2: Cleartext email preservation
  - Validation 3: Domain extraction accuracy
  - Validation 4: Required fields completeness
  - Validation 5: HEM format validation (SHA256)

- **07_update_dependent_views.sql**
  - Updates `temp_vector_emails` view to new schema
  - Verifies view structure and record counts

- **08_create_dynamodb_export_view.sql**
  - Creates `v_dynamodb_email_export` view
  - Implements canonical email selection logic
  - Returns one record per VUP
  - Prioritizes business > personal, cleartext > hash-only

- **09_verify_access_patterns.sql**
  - Tests 4 critical access patterns
  - Verifies query performance (EXPLAIN plans)
  - Validates canonical email selection
  - Checks DISTKEY/SORTKEY effectiveness

- **10_create_v_email_hem_best.sql** (Andrew's Revision)
  - Creates `base.v_email_hem_best` view for HEM-centric export
  - One row per HEM (strict uniqueness requirement)
  - Applies ENG-1914 winner logic: jobs → recency → lowest vup_id
  - Includes unresolved HEMs (vup_id IS NULL) for cleartext resolution
  - Uses late binding (NO SCHEMA BINDING) for schema evolution

- **11_validate_v_email_hem_best.sql** (Andrew's Revision)
  - Validation 1: One row per HEM uniqueness
  - Validation 2: Unresolved HEM coverage
  - Validation 3: Deterministic tie-breaking consistency
  - Validation 4: Source table coverage parity
  - Validation 5: Cleartext preference enforcement
  - Sample tie-breaking decisions for manual review

- **12_dynamodb_table_and_loader.md** (DynamoDB Infrastructure)
  - DynamoDB table structure (vup_id/hem primary key)
  - Global secondary index (hem-index for HEM lookups)
  - Sharded sentinel pattern for unresolved HEMs
  - AWS CLI commands for table creation
  - Lambda function for S3 to DynamoDB ingestion
  - Access pattern validation queries
  - Operational procedures and monitoring
  - Cost analysis and security considerations

- **99_rollback.sql**
  - Complete rollback procedures
  - Restores original table from backup
  - Recreates original views
  - Verifies restoration completeness

---

## Access Patterns

### Pattern 1: HEM → VUP Lookup
**Performance:** Fast (DISTKEY optimization)
```sql
SELECT hem, vup_id, email_type
FROM derived.vector_email_new
WHERE hem = '<sha256_hash>';
```

### Pattern 2: HEM → Cleartext Email
**Performance:** Fast (DISTKEY optimization)
```sql
SELECT hem, email, domain
FROM derived.vector_email_new
WHERE hem = '<sha256_hash>' AND email IS NOT NULL;
```

### Pattern 3: VUP → All HEMs
**Performance:** Efficient (SORTKEY optimization)
```sql
SELECT vup_id, hem, email, email_type
FROM derived.vector_email_new
WHERE vup_id = '<vup_id>';
```

### Pattern 4: VUP → Canonical Email
**Performance:** Optimal (dedicated view)
```sql
SELECT vup_id, canonical_email, canonical_email_type
FROM derived.v_dynamodb_email_export
WHERE vup_id = '<vup_id>';
```

---

## Support

### Questions or Issues?

**Technical Questions:**
- Review `ENG-1973_SUMMARY.md` for detailed explanations
- Check `00_production_schema_verification.md` for baseline metrics
- Review individual SQL file comments for step-by-step instructions

**Execution Issues:**
- Check validation results from `06_data_quality_validation.sql`
- Review migration metadata: `SELECT * FROM derived.vector_email_migration_metadata_eng1973 ORDER BY execution_timestamp;`
- If stuck, execute `99_rollback.sql` to restore original state

**Rollback Decision:**
- Execute rollback if any validation fails
- Execute rollback if downstream systems break
- Execute rollback if unacceptable data loss detected
- Keep backup table for 30 days before cleanup

---

## Success Criteria

### Migration Complete When:

1. ✅ All 5 validations pass (06_data_quality_validation.sql)
2. ✅ All 4 access patterns work efficiently (09_verify_access_patterns.sql)
3. ✅ Downstream systems tested (Cube.js, DynamoDB, CRM)
4. ✅ Stakeholder approval obtained
5. ✅ 30-day observation period completed with no issues

---

