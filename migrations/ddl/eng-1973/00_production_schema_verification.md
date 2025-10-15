# ENG-1973 Production Schema Verification

---

## Current Schema Structure

Execute the following queries and document results:

### 1. Column Definitions

```sql
SELECT 
    column_name,
    data_type,
    character_maximum_length,
    is_nullable,
    column_default,
    encoding,
    distkey,
    sortkey
FROM svv_columns
WHERE schema_name = 'derived'
  AND table_name = 'vector_email'
ORDER BY ordinal_position;
```

**Results:**
```
[Paste query results here]
```

---

### 2. Distribution and Sort Keys

```sql
SELECT 
    tablename,
    diststyle,
    sortkey1,
    sortkey_num
FROM pg_table_def
WHERE schemaname = 'derived'
  AND tablename = 'vector_email'
LIMIT 1;
```

**Results:**
```
[Paste query results here]
```

---

### 3. Current Data Metrics

```sql
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT sha256) as unique_hems,
    COUNT(DISTINCT vup_id) as unique_vups,
    COUNT(CASE WHEN email IS NOT NULL THEN 1 END) as records_with_cleartext,
    COUNT(CASE WHEN email IS NULL THEN 1 END) as records_hash_only,
    ROUND(100.0 * COUNT(CASE WHEN email IS NOT NULL THEN 1 END) / COUNT(*), 2) as pct_with_cleartext
FROM derived.vector_email;
```

**Results:**
```
[Paste query results here]
```

---

### 4. Current Schema Issues

**Expected Issues to Document:**
1. HEMs with multiple VUPs (canonical VUP ambiguity)
2. Same HEM with both cleartext and hash-only records
3. Exact duplicate rows
4. VUPs with multiple business emails (canonical email ambiguity)

---

## Baseline Established

- [ ] Current schema documented
- [ ] Distribution keys documented
- [ ] Current record counts documented
- [ ] Stakeholders notified of baseline metrics


---

**Next Step:** Execute `01_analyze_current_data_quality.sql`
