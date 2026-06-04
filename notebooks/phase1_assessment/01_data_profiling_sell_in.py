# Databricks notebook source
# Phase 1 — Data Profiling: Sell-In
# Run this on your WORK COMPUTER in Databricks.
# Output is written to: docs/phase_outputs/phase1_data_inventory.md
# Commit that file and push — the agent reads it on your personal laptop.

# COMMAND ----------
# MAGIC %md
# MAGIC # Phase 1 · Data Profiling — Sell-In
# MAGIC **Purpose:** Understand the sell-in source before any schema or model design.
# MAGIC
# MAGIC **After running:** Commit `docs/phase_outputs/phase1_data_inventory.md` and push to GitHub.

# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import *
from datetime import datetime
import json

# ── Configuration ──────────────────────────────────────────────────────────
ENVIRONMENT   = "dev"         # Change to "staging" or "prod" as needed
SNOWFLAKE_DB  = f"MGI_{ENVIRONMENT.upper()}"
SOURCE_NAME   = "SELL_IN"
SOURCE_TABLE  = f"{SNOWFLAKE_DB}.BRONZE.SELL_IN_RAW"   # Adjust to your actual table name
OUTPUT_PATH   = "/Workspace/Repos/Market-Driven-Commercial-Growth-Intelligence-Platform/docs/phase_outputs/phase1_data_inventory.md"
# Or use DBFS:
# OUTPUT_PATH = "dbfs:/mnt/repo/docs/phase_outputs/phase1_data_inventory.md"

RUN_AT = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

# COMMAND ----------
# MAGIC %md ## 1. Load Source Table

# COMMAND ----------
df = spark.table(SOURCE_TABLE)

total_rows  = df.count()
total_cols  = len(df.columns)
schema_json = df.schema.json()

print(f"Table: {SOURCE_TABLE}")
print(f"Rows: {total_rows:,}  |  Columns: {total_cols}")

# COMMAND ----------
# MAGIC %md ## 2. Schema Snapshot

# COMMAND ----------
print("Column inventory:")
for field in df.schema.fields:
    print(f"  {field.name:<40} {str(field.dataType):<25} nullable={field.nullable}")

# COMMAND ----------
# MAGIC %md ## 3. Null Rate per Column

# COMMAND ----------
null_rates = {}
for col in df.columns:
    null_count = df.filter(F.col(col).isNull()).count()
    null_pct   = round(null_count / total_rows * 100, 2) if total_rows > 0 else 0
    null_rates[col] = {"null_count": null_count, "null_pct": null_pct}
    flag = "⚠️ HIGH" if null_pct > 5 else ("🔶 WARN" if null_pct > 1 else "✅ OK")
    print(f"  {col:<40} {null_pct:>6.2f}%  {flag}")

# COMMAND ----------
# MAGIC %md ## 4. Date Range & Temporal Continuity

# COMMAND ----------
# Adjust date column name to match your actual schema
DATE_COL = "ship_date"   # ← change if your column is named differently

if DATE_COL in df.columns:
    date_stats = df.agg(
        F.min(DATE_COL).alias("min_date"),
        F.max(DATE_COL).alias("max_date"),
        F.countDistinct(DATE_COL).alias("distinct_dates")
    ).collect()[0]

    print(f"Date range: {date_stats['min_date']} → {date_stats['max_date']}")
    print(f"Distinct dates: {date_stats['distinct_dates']}")

    # Check for gaps > 2 consecutive weeks
    weekly = df.withColumn("week", F.date_trunc("week", F.col(DATE_COL))) \
               .groupBy("week").count().orderBy("week")
    week_count = weekly.count()
    print(f"Distinct weeks in data: {week_count}")
else:
    date_stats = {"min_date": "N/A", "max_date": "N/A", "distinct_dates": 0}
    print(f"⚠️ Column '{DATE_COL}' not found — update DATE_COL variable")

# COMMAND ----------
# MAGIC %md ## 5. Volume Sanity & Key Field Cardinality

# COMMAND ----------
# Adjust column names to your actual schema
KEY_COLS = {
    "sku_id":       "sku_id",        # ← change if needed
    "customer_id":  "customer_id",   # ← change if needed
    "units":        "units_shipped", # ← change if needed
    "revenue":      "net_revenue"    # ← change if needed
}

cardinality = {}
for label, col in KEY_COLS.items():
    if col in df.columns:
        n = df.select(col).distinct().count()
        cardinality[label] = n
        print(f"  {label} ({col}): {n:,} distinct values")
    else:
        cardinality[label] = "COLUMN NOT FOUND"
        print(f"  ⚠️ {label}: column '{col}' NOT FOUND in source")

# COMMAND ----------
# MAGIC %md ## 6. Value Range Check (Units & Revenue)

# COMMAND ----------
numeric_stats = {}
for label, col in KEY_COLS.items():
    if col in df.columns and label in ("units", "revenue"):
        stats = df.agg(
            F.min(col).alias("min"),
            F.max(col).alias("max"),
            F.mean(col).alias("mean"),
            F.sum(col).alias("total")
        ).collect()[0]
        numeric_stats[label] = dict(stats.asDict())
        print(f"  {label} — min:{stats['min']:,.0f}  max:{stats['max']:,.0f}  mean:{stats['mean']:,.1f}  total:{stats['total']:,.0f}")

# COMMAND ----------
# MAGIC %md ## 7. Duplicate Check

# COMMAND ----------
# Define your expected unique key (adjust columns as needed)
UK_COLS = ["sku_id", "customer_id", DATE_COL]
existing_uk = [c for c in UK_COLS if c in df.columns]

if len(existing_uk) == len(UK_COLS):
    dup_count = total_rows - df.dropDuplicates(existing_uk).count()
    dup_flag  = "⚠️ DUPLICATES FOUND" if dup_count > 0 else "✅ No duplicates"
    print(f"Duplicate rows on {existing_uk}: {dup_count:,}  {dup_flag}")
else:
    dup_count = "NOT CHECKED"
    print(f"⚠️ Could not check duplicates — some key columns not found: {UK_COLS}")

# COMMAND ----------
# MAGIC %md ## 8. Sample Rows

# COMMAND ----------
display(df.limit(5))

# COMMAND ----------
# MAGIC %md ## 9. Write Output File
# MAGIC
# MAGIC This file is committed and pushed to GitHub so the agent can read it.

# COMMAND ----------
null_table_rows = "\n".join([
    f"| `{col}` | {v['null_count']:,} | {v['null_pct']}% | {'⚠️ HIGH' if v['null_pct'] > 5 else ('🔶 WARN' if v['null_pct'] > 1 else '✅ OK')} |"
    for col, v in null_rates.items()
])

schema_table_rows = "\n".join([
    f"| `{f.name}` | `{str(f.dataType)}` | {f.nullable} |"
    for f in df.schema.fields
])

cardinality_rows = "\n".join([
    f"| `{col}` | {val:,} |" if isinstance(val, int) else f"| `{col}` | ⚠️ {val} |"
    for col, val in cardinality.items()
])

output_md = f"""# Phase 1 — Data Inventory: {SOURCE_NAME}

**Generated:** {RUN_AT}
**Environment:** {ENVIRONMENT.upper()}
**Source table:** `{SOURCE_TABLE}`

---

## Summary

| Metric | Value |
|--------|-------|
| Total rows | {total_rows:,} |
| Total columns | {total_cols} |
| Date range | {date_stats.get('min_date', 'N/A')} → {date_stats.get('max_date', 'N/A')} |
| Distinct date periods | {date_stats.get('distinct_dates', 'N/A')} |
| Duplicate rows | {dup_count if isinstance(dup_count, str) else f'{dup_count:,}'} |

## Schema

| Column | Type | Nullable |
|--------|------|---------|
{schema_table_rows}

## Null Rates

| Column | Null Count | Null % | Status |
|--------|-----------|--------|--------|
{null_table_rows}

## Key Field Cardinality

| Field | Distinct Count |
|-------|---------------|
{cardinality_rows}

## Volume Stats

```
{json.dumps(numeric_stats, indent=2, default=str)}
```

## Open Items (fill these in manually after reviewing output)

- [ ] Are any columns missing that were expected? If so, which ones?
- [ ] Are null rates acceptable for key fields?
- [ ] Does the date range match expectations?
- [ ] Are there any obvious data quality anomalies in the sample rows?
- [ ] Does total sell-in volume match business expectation (order of magnitude)?

## Readiness Score for SELL_IN: [0-100 — fill in after reviewing]

**Score:** [TBD]
**Rationale:** [TBD]
"""

# Write to Databricks Repo path (adjust if using DBFS or a different path)
with open(OUTPUT_PATH.replace("/Workspace/Repos/Market-Driven-Commercial-Growth-Intelligence-Platform", "."), "w") as f:
    f.write(output_md)

print(f"\n✅ Output written.")
print(f"Next step on your work computer:")
print(f"  git add docs/phase_outputs/phase1_data_inventory.md")
print(f"  git commit -m 'data: phase1 sell_in profiling'")
print(f"  git push origin main")
print(f"Then tell the agent: 'sell_in profiling done, results committed'")
