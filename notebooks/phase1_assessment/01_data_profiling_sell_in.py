# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Data Profiling — Sell-In
# MAGIC
# MAGIC **Purpose:** Understand the sell-in source before any schema or model design.
# MAGIC
# MAGIC **Before running:**
# MAGIC 1. Run `00_connection_test.py` first — confirm connection is working
# MAGIC 2. Update `DB_NAME`, `SCHEMA_NAME`, and `TABLE_NAME` below to your actual source
# MAGIC
# MAGIC **After running:** Commit `docs/phase_outputs/phase1_data_inventory.md` and push to GitHub.
# MAGIC Then tell the agent: "sell_in profiling done, results committed"

# COMMAND ----------
# MAGIC %run ../utils/execution_logger

# COMMAND ----------
import time as _time
_NB_START    = _time.time()
ENVIRONMENT = "dev"
_nb_errors   = []
_nb_warnings = []

# COMMAND ----------
# MAGIC %md ## Configuration — UPDATE THESE

# COMMAND ----------
# ── UPDATE to match your Snowflake source ───────────────────────────────────
DB_NAME     = "YOUR_SOURCE_DATABASE"   # e.g. "MDP_PRD"
SCHEMA_NAME = "YOUR_SOURCE_SCHEMA"     # e.g. "COMMERCIAL"
TABLE_NAME  = "YOUR_SELL_IN_TABLE"     # e.g. "FACT_SELL_IN" or "VW_SELL_IN"

# ── Column name mapping — UPDATE to match your table's actual column names ──
COL_DATE     = "ship_date"       # The shipment/transaction date column
COL_SKU      = "sku_id"          # SKU / product identifier
COL_CUSTOMER = "customer_id"     # Customer / retailer identifier
COL_UNITS    = "units_shipped"   # Volume metric
COL_REVENUE  = "net_revenue"     # Revenue metric (set to None if not present)

# ── Output ───────────────────────────────────────────────────────────────────
OUTPUT_FILE = "docs/phase_outputs/phase1_data_inventory.md"   # relative to repo root

# COMMAND ----------
# MAGIC %md ## Connection — Azure Key Vault

# COMMAND ----------
KEYVAULT_NAME = "DAN-AM-P-KVT800-R-MDP-DB"
KEY_NAME_USR  = "snowflake-user"
KEY_NAME_PWD  = "snowflake-password"
SF_URL        = "danonenam.east-us-2.azure.snowflakecomputing.com"
SF_WAREHOUSE  = "PRD_MDP_ANL_WH"
SF_ROLE       = "PRD_MDP"

try:
    user     = dbutils.secrets.get(scope=KEYVAULT_NAME, key=KEY_NAME_USR)
    password = dbutils.secrets.get(scope=KEYVAULT_NAME, key=KEY_NAME_PWD)
    print(f"✅ Credentials retrieved from: {KEYVAULT_NAME}")
except NameError:
    print("🚨 Non-Databricks env — using MOCK credentials")
    user, password = "MOCK_USER", "MOCK_PASSWORD"
except Exception as e:
    print(f"🚨 Secret error: {e}. Run 00_connection_test.py first.")
    user, password = "MOCK_USER", "MOCK_PASSWORD"

sfOptions = {
    "sfURL": SF_URL, "sfUser": user, "sfPassword": password,
    "sfDatabase": DB_NAME, "sfSchema": SCHEMA_NAME, "sfWarehouse": SF_WAREHOUSE, "sfRole": SF_ROLE,
}

# COMMAND ----------
# MAGIC %md ## 1. Load Table

# COMMAND ----------
import pyspark.sql.functions as F
from datetime import datetime

RUN_AT = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
SOURCE_LABEL = f"{DB_NAME}.{SCHEMA_NAME}.{TABLE_NAME}"

print(f"Loading: {SOURCE_LABEL}")
try:
    df = spark.read.format("snowflake").options(**sfOptions).option("dbtable", TABLE_NAME).load()
except Exception as _load_err:
    _nb_errors.append(make_error(
        step        = "Step 1 — Load Table",
        category    = "TABLE_NOT_FOUND",
        severity    = "CRITICAL",
        message     = f"Could not load table {SOURCE_LABEL}: {str(_load_err)[:200]}",
        raw_exception = str(_load_err),
        resolution  = "Verify DB_NAME, SCHEMA_NAME, TABLE_NAME are correct",
        is_blocking = True,
    ))
    write_execution_log(
        notebook_id  = "01_data_profiling_sell_in",
        source_name  = "SELL_IN",
        status       = "ERROR",
        duration_sec = _time.time() - _NB_START,
        errors       = _nb_errors,
        warnings     = _nb_warnings,
        metrics      = {"source": SOURCE_LABEL},
        output_files = [],
        environment  = ENVIRONMENT,
    )
    dbutils.notebook.exit("TABLE_NOT_FOUND")

total_rows = df.count()
total_cols = len(df.columns)
print(f"\n  Rows:    {total_rows:,}")
print(f"  Columns: {total_cols}")

# COMMAND ----------
# MAGIC %md ## 2. Schema Snapshot

# COMMAND ----------
print("Column inventory:")
schema_rows = []
for field in df.schema.fields:
    print(f"  {field.name:<45} {str(field.dataType):<25} nullable={field.nullable}")
    schema_rows.append(f"| `{field.name}` | `{str(field.dataType)}` | {field.nullable} |")

# COMMAND ----------
# MAGIC %md ## 3. Null Rate per Column

# COMMAND ----------
print(f"Null rates ({total_rows:,} total rows):")
null_results = []
for col in df.columns:
    null_count = df.filter(F.col(col).isNull()).count()
    null_pct   = round(null_count / total_rows * 100, 2) if total_rows > 0 else 0
    flag       = "⚠️  HIGH" if null_pct > 5 else ("🔶 WARN" if null_pct > 1 else "✅  OK")
    null_results.append({"col": col, "null_count": null_count, "null_pct": null_pct, "flag": flag})
    print(f"  {col:<45} {null_pct:>6.2f}%  {flag}")
    if null_pct > 5:
        _nb_warnings.append(f"High null rate: {col} = {null_pct}%")

# COMMAND ----------
# MAGIC %md ## 4. Date Range & Temporal Coverage

# COMMAND ----------
date_info = {"min": "N/A", "max": "N/A", "distinct": "N/A"}
if COL_DATE in df.columns:
    stats = df.agg(
        F.min(COL_DATE).alias("min"), F.max(COL_DATE).alias("max"),
        F.countDistinct(COL_DATE).alias("distinct")
    ).collect()[0]
    date_info = {"min": str(stats["min"]), "max": str(stats["max"]), "distinct": stats["distinct"]}
    print(f"Date range:     {date_info['min']} → {date_info['max']}")
    print(f"Distinct dates: {date_info['distinct']:,}")

    # Check for weekly gaps
    weekly_counts = df.withColumn("week", F.date_trunc("week", F.col(COL_DATE))) \
                      .groupBy("week").agg(F.count("*").alias("row_count")).orderBy("week")
    week_total = weekly_counts.count()
    print(f"Distinct weeks: {week_total:,}")

    # Flag gaps > 2 consecutive weeks
    from pyspark.sql.window import Window
    w = Window.orderBy("week")
    gaps = weekly_counts \
        .withColumn("prev_week", F.lag("week").over(w)) \
        .withColumn("gap_weeks", F.datediff("week", "prev_week") / 7) \
        .filter(F.col("gap_weeks") > 2)
    gap_count = gaps.count()
    if gap_count > 0:
        print(f"\n  ⚠️  {gap_count} gap(s) > 2 consecutive weeks detected:")
        gaps.show(10, truncate=False)
    else:
        print(f"  ✅ No gaps > 2 consecutive weeks")
else:
    _nb_warnings.append(f"Date column '{COL_DATE}' not found — update COL_DATE variable")
    print(f"  ⚠️  Date column '{COL_DATE}' not found — update COL_DATE variable")

# COMMAND ----------
# MAGIC %md ## 5. Key Field Cardinality

# COMMAND ----------
cardinality = {}
for label, col in [("SKU", COL_SKU), ("Customer", COL_CUSTOMER)]:
    if col and col in df.columns:
        n = df.select(col).distinct().count()
        cardinality[label] = n
        print(f"  Distinct {label}s ({col}): {n:,}")
    elif col:
        cardinality[label] = "COLUMN NOT FOUND"
        print(f"  ⚠️  {label} column '{col}' not found")

# COMMAND ----------
# MAGIC %md ## 6. Volume Range Check

# COMMAND ----------
numeric_stats = {}
for label, col in [("Units", COL_UNITS), ("Revenue", COL_REVENUE)]:
    if col and col in df.columns:
        stats = df.agg(
            F.min(col).alias("min"), F.max(col).alias("max"),
            F.mean(col).alias("mean"), F.sum(col).alias("total")
        ).collect()[0]
        numeric_stats[label] = {k: float(v) if v is not None else None for k, v in stats.asDict().items()}
        print(f"  {label} ({col})")
        print(f"    min={stats['min']:,.0f}  max={stats['max']:,.0f}  mean={stats['mean']:,.1f}  total={stats['total']:,.0f}")
        # Flag impossible values
        neg = df.filter(F.col(col) < 0).count()
        if neg > 0:
            print(f"    ⚠️  {neg:,} negative values — investigate")
    elif col:
        print(f"  ⚠️  Column '{col}' not found — update COL_UNITS/COL_REVENUE variables")

# COMMAND ----------
# MAGIC %md ## 7. Duplicate Check

# COMMAND ----------
uk_cols = [c for c in [COL_DATE, COL_SKU, COL_CUSTOMER] if c and c in df.columns]
if len(uk_cols) == 3:
    deduped   = df.dropDuplicates(uk_cols).count()
    dup_count = total_rows - deduped
    flag = f"⚠️  {dup_count:,} duplicates found" if dup_count > 0 else "✅ No duplicates"
    print(f"Duplicate check on {uk_cols}: {flag}")
else:
    dup_count = "NOT CHECKED — some key columns not found"
    print(f"⚠️  Could not check duplicates: {dup_count}")

# COMMAND ----------
# MAGIC %md ## 8. Sample Rows

# COMMAND ----------
display(df.limit(10))

# COMMAND ----------
# MAGIC %md ## 9. Write Output File

# COMMAND ----------
null_table = "\n".join([
    f"| `{r['col']}` | {r['null_count']:,} | {r['null_pct']}% | {r['flag']} |"
    for r in null_results
])

schema_table = "\n".join(schema_rows)

card_table = "\n".join([
    f"| {label} | {val:,} |" if isinstance(val, int) else f"| {label} | ⚠️ {val} |"
    for label, val in cardinality.items()
])

output_md = f"""# Phase 1 — Data Inventory: SELL_IN

**Generated:** {RUN_AT}
**Source:** `{SOURCE_LABEL}`

---

## Summary

| Metric | Value |
|--------|-------|
| Total rows | {total_rows:,} |
| Total columns | {total_cols} |
| Date range | {date_info['min']} → {date_info['max']} |
| Distinct date periods | {date_info['distinct']} |
| Duplicate rows | {dup_count if isinstance(dup_count, str) else f'{dup_count:,}'} |

## Schema

| Column | Type | Nullable |
|--------|------|---------|
{schema_table}

## Null Rates

| Column | Null Count | Null % | Status |
|--------|-----------|--------|--------|
{null_table}

## Key Field Cardinality

| Field | Distinct Count |
|-------|---------------|
{card_table}

## Volume Stats (Units & Revenue)

| Metric | Min | Max | Mean | Total |
|--------|-----|-----|------|-------|
{chr(10).join([f"| {label} | {s.get('min', 'N/A'):,.0f} | {s.get('max', 'N/A'):,.0f} | {s.get('mean', 'N/A'):,.1f} | {s.get('total', 'N/A'):,.0f} |" for label, s in numeric_stats.items()])}

## Open Items — Fill in after reviewing output

- [ ] Are all expected columns present? Any surprises in the schema?
- [ ] Are null rates acceptable for key fields (SKU, customer, date, units)?
- [ ] Does the date range match expectations?
- [ ] Are the distinct SKU/customer counts plausible?
- [ ] Does total sell-in volume match business expectation (order of magnitude)?
- [ ] Were any gaps > 2 weeks detected? If so, document the resolution decision.
- [ ] Were any negative unit/revenue values found? If so, document the cause.

## Readiness Score for SELL_IN

**Score:** [0–100 — fill in after reviewing]
**Rationale:** [explain the score]
"""

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    f.write(output_md)

print(f"✅ Output written to: {OUTPUT_FILE}")
print("")
print("Next steps on your work computer:")
print("  git add docs/phase_outputs/phase1_data_inventory.md")
print("  git commit -m 'data: phase1 sell_in profiling'")
print("  git push origin main")
print("")
print("Then tell the agent: 'sell_in profiling done, results committed'")

# COMMAND ----------
# MAGIC %md ## Execution Log

# COMMAND ----------
_nb_status  = "ERROR" if _nb_errors else ("PARTIAL" if _nb_warnings else "SUCCESS")
_nb_metrics = {
    "source":          SOURCE_LABEL,
    "total_rows":      total_rows,
    "total_cols":      total_cols,
    "date_min":        date_info.get("min"),
    "date_max":        date_info.get("max"),
    "date_distinct":   date_info.get("distinct"),
    "dup_count":       dup_count if isinstance(dup_count, int) else -1,
    "cardinality_sku": cardinality.get("SKU"),
    "cardinality_customer": cardinality.get("Customer"),
    "high_null_cols":  [r["col"] for r in null_results if r["null_pct"] > 5],
    "output_file":     OUTPUT_FILE,
}
write_execution_log(
    notebook_id  = "01_data_profiling_sell_in",
    source_name  = "SELL_IN",
    status       = _nb_status,
    duration_sec = _time.time() - _NB_START,
    errors       = _nb_errors,
    warnings     = _nb_warnings,
    metrics      = _nb_metrics,
    output_files = [OUTPUT_FILE],
    environment  = ENVIRONMENT,
)
