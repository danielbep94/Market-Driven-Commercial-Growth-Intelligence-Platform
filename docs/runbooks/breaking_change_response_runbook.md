# Breaking Change Response Runbook

**Trigger:** A source system schema change causes Bronze ingestion to fail schema validation.  
**Owner:** Data Engineering  
**Response SLA:** Acknowledge within 30 minutes; resolution plan within 2 hours.

---

## What happens automatically

When a Bronze notebook detects a schema mismatch against the data contract:
1. **Ingestion stops immediately** — no partial data is written to Bronze
2. **Alert fires** to the data engineer and data steward
3. **DQ_LOG entry written** with `check_type = SCHEMA_VALIDATION`, `check_result = ERROR`, and details of the mismatch
4. **No Silver or Gold processing runs** for this source until resolved

This is the `breaking_change_protocol` defined in each data contract.

---

## Response Steps

### Step 1: Identify the change
Compare the incoming file schema against the data contract:
```python
# The Bronze notebook logs the exact mismatch:
# Expected columns: [list from contract]
# Received columns: [list from file]
# Missing: [columns in contract not in file]
# New: [columns in file not in contract]
```

### Step 2: Classify the change

| Change type | Severity | Action |
|-------------|---------|--------|
| Column renamed | HIGH | Update homologation and data contract; test Silver mapping |
| Column removed | HIGH | Assess downstream impact; update contract and Silver |
| Column added | LOW | Add to Bronze ingest; update contract; may not need Silver change |
| Data type changed | MEDIUM | Assess cast compatibility; update contract |
| Encoding change | MEDIUM | Update Bronze ingest; validate Silver output |

### Step 3: Update the data contract
Update `docs/data_contracts/contract_[source].md` to reflect the new schema.
Create a PR. Requires reviewer approval before merge.

### Step 4: Update Bronze/Silver notebooks if needed
Test the change in DEV. Validate Silver output row counts and key field values.

### Step 5: Communicate
- If the delay will impact the Monday refresh SLA: notify BI Developer and PM
- If resolved within SLA: proceed normally
- If not resolved within SLA: dashboard shows prior week data with "Data as of [prior week]" banner

### Step 6: Re-enable ingestion
After contract and code are updated and tested in DEV:
1. Rerun Bronze notebook with the corrected schema
2. Verify DQ_LOG shows PASS for schema validation
3. Run Silver and Gold for the affected source
4. Verify MART row counts
