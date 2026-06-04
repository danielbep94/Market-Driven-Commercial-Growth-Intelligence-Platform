# Pipeline Failure Runbook

**Owner:** Data Engineering  
**Last reviewed:** [DATE]

## When a pipeline fails

### Step 1: Identify the failure stage
Check the DQ_LOG table:
```sql
SELECT * FROM MGI_PROD.MONITORING.DQ_LOG
WHERE check_result = 'ERROR'
ORDER BY check_timestamp DESC
LIMIT 20;
```

### Step 2: Identify the root cause

| Error type | Likely cause | Action |
|-----------|-------------|--------|
| `ROW_COUNT_DROP` | Source file not delivered or truncated | Check source SLA; contact technical owner |
| `NULL_RATE_EXCEEDED` | Source system changed schema or export | Compare current schema to data contract |
| `REFERENTIAL_INTEGRITY` | New SKUs or customers not in dimension tables | Run homologation; check HOMOLOGATION_EXCEPTIONS |
| `DUPLICATE_BATCH` | Pipeline ran twice with same batch_id | Safe to ignore if batch_id check is working; verify Bronze counts |

### Step 3: Decide whether to hold or continue
- **Hold** if: Source data is missing or structurally corrupted. Dashboard data from prior week is better than corrupted data.
- **Continue with prior week** if: Source is late but prior week data is complete. Flag dashboard as "Data as of [prior week]".
- **Continue with flags** if: Data is partially available. Set `data_confidence_overall = LOW` for affected brands.

### Step 4: Communicate
- Notify BI Developer if Gold refresh will be delayed past 6:00 AM Monday.
- Notify PM if dashboard will not be available by 8:00 AM Monday.
- Notify Business Owner of affected source per SLA breach process.

### Step 5: Fix and rerun
- Fix the root cause in Bronze (never modify Bronze data — fix the ingestion job).
- Rerun Silver and Gold for affected source only (not full pipeline).
- Verify row counts in MART match expected before marking pipeline as complete.
