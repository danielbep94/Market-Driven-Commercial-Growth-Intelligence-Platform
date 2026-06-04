# Databricks notebook source
# MAGIC %md
# MAGIC # 00b · Source Discovery & Validation
# MAGIC
# MAGIC **Pattern:** You define the SQL — the notebook profiles whatever comes back.
# MAGIC
# MAGIC ## How to use
# MAGIC 1. Fill in the `SOURCES` dictionary below with your SQL definitions
# MAGIC 2. Leave `None` for sources not yet defined — notebook skips them with a flag
# MAGIC 3. Run All
# MAGIC 4. Review the output printed here
# MAGIC 5. Commit `docs/phase_outputs/phase1_data_inventory.md` and push
# MAGIC 6. Tell the agent: **"discovery done, inventory committed"**
# MAGIC
# MAGIC The agent reads the inventory and generates Bronze DDL + data contracts per source.

# COMMAND ----------
# MAGIC %md ## ─── EDIT THIS SECTION ─────────────────────────────────────────
# MAGIC Add or update SQL definitions below. Leave `None` for sources not ready yet.

# COMMAND ----------

SOURCES = {
    # ── 1 · Investment / Marketing ──────────────────────────────────────────
    "DATA_MKT":
        "SELECT * FROM PRD_MDP.MDP_DSP.VW_MKT_ECOMM WHERE anio >= 2024",

    # ── 2 · Sell-In ──────────────────────────────────────────────────────────
    "DATA_SELL_IN": None,          # ← paste your SQL when ready

    # ── 3 · Sell-Out ─────────────────────────────────────────────────────────
    "DATA_SELL_OUT": None,

    # ── 4 · Waste / Merma ────────────────────────────────────────────────────
    "DATA_WASTE": None,

    # ── 5 · Demand Forecast ──────────────────────────────────────────────────
    "DATA_FORECAST": None,

    # ── 6 · Nielsen / Market Share ───────────────────────────────────────────
    "DATA_NIELSEN": None,

    # ── 7 · Price ────────────────────────────────────────────────────────────
    "DATA_PRICE": None,

    # ── 8 · Promotions ───────────────────────────────────────────────────────
    "DATA_PROMO": None,

    # ── 9 · Inventory / Stock ────────────────────────────────────────────────
    "DATA_INVENTORY": None,

    # ── 10 · Calendar / Date Dimension ──────────────────────────────────────
    "DATA_CALENDAR": None,
}

# ── Domain labels (for the output report) ────────────────────────────────────
DOMAIN_LABELS = {
    "DATA_MKT":       "Investment / Marketing",
    "DATA_SELL_IN":   "Sell-In",
    "DATA_SELL_OUT":  "Sell-Out",
    "DATA_WASTE":     "Waste / Merma",
    "DATA_FORECAST":  "Demand Forecast",
    "DATA_NIELSEN":   "Nielsen / Market Share",
    "DATA_PRICE":     "Price",
    "DATA_PROMO":     "Promotions",
    "DATA_INVENTORY": "Inventory / Stock",
    "DATA_CALENDAR":  "Calendar / Date Dimension",
}

# MAGIC %md ## ─── DO NOT EDIT BELOW THIS LINE ──────────────────────────────

# COMMAND ----------
# MAGIC %md ## Connection

# COMMAND ----------
KEYVAULT_NAME = "DAN-AM-P-KVT800-R-MDP-DB"
KEY_NAME_USR  = "snowflake-user"
KEY_NAME_PWD  = "snowflake-password"
SF_URL        = "danonenam.east-us-2.azure.snowflakecomputing.com"
SF_WAREHOUSE  = "PRD_MDP_ANL_WH"

# Default context — fully-qualified SQL in SOURCES can override this
DEFAULT_DB     = "PRD_MDP"
DEFAULT_SCHEMA = "MDP_DSP"

try:
    user     = dbutils.secrets.get(scope=KEYVAULT_NAME, key=KEY_NAME_USR)
    password = dbutils.secrets.get(scope=KEYVAULT_NAME, key=KEY_NAME_PWD)
    print(f"✅ Credentials loaded from: {KEYVAULT_NAME}")
except NameError:
    user, password = "MOCK_USER", "MOCK_PASSWORD"
    print("🚨 Non-Databricks env — using MOCK credentials")
except Exception as e:
    user, password = "MOCK_USER", "MOCK_PASSWORD"
    print(f"🚨 Secret error: {e}")

from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql.types import NumericType, DateType, TimestampType

RUN_AT      = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
OUTPUT_FILE = "docs/phase_outputs/phase1_data_inventory.md"

# Base Snowflake connector options
BASE_OPTS = {
    "sfURL":       SF_URL,
    "sfUser":      user,
    "sfPassword":  password,
    "sfDatabase":  DEFAULT_DB,
    "sfSchema":    DEFAULT_SCHEMA,
    "sfWarehouse": SF_WAREHOUSE,
}

def run_source_sql(sql: str):
    """Execute any SQL (can reference any fully-qualified Snowflake table/view)."""
    return spark.read.format("snowflake") \
               .options(**BASE_OPTS) \
               .option("query", sql) \
               .load()

def detect_date_col(columns: list) -> str | None:
    """
    Auto-detect the temporal column.
    Priority: anio > año > year > periodo > fecha > date > timestamp columns.
    Returns column name or None.
    """
    priority = ["anio", "año", "year", "yr", "periodo", "period",
                "fecha", "fecha_proceso", "fecha_venta",
                "date", "dt", "week", "semana", "mes", "month"]
    col_lower = {c.lower(): c for c in columns}
    for p in priority:
        if p in col_lower:
            return col_lower[p]
    return None

def score_source(null_flags, has_date, has_numeric, dup_pct) -> int:
    """
    Compute 0-100 readiness score.
    Completeness 25 | Temporal 25 | Volume 25 | Cardinality/Dups 25
    """
    # Completeness: deduct 5 per HIGH null, 2 per WARN null
    high_nulls = sum(1 for f in null_flags if "HIGH" in f)
    warn_nulls = sum(1 for f in null_flags if "WARN" in f)
    completeness = max(0, 25 - (high_nulls * 5) - (warn_nulls * 2))

    temporal   = 25 if has_date else 5
    volume     = 25 if has_numeric else 15
    cardinality = max(0, 25 - min(25, int(dup_pct / 2)))  # -2 per 1% duplicates

    return completeness + temporal + volume + cardinality

# COMMAND ----------
# MAGIC %md ## Run Validation Per Source

# COMMAND ----------
defined   = {k: v for k, v in SOURCES.items() if v is not None}
undefined = {k: v for k, v in SOURCES.items() if v is None}

print(f"Sources defined:   {len(defined)}/10")
print(f"Sources pending:   {len(undefined)}/10")
if undefined:
    print(f"  Pending: {', '.join(undefined.keys())}")
print()

results = {}   # key → validation result dict

for source_key, sql in defined.items():
    label = DOMAIN_LABELS.get(source_key, source_key)
    print("\n" + "═" * 70)
    print(f"  {source_key}  —  {label}")
    print("═" * 70)
    print(f"  SQL: {sql[:120]}{'...' if len(sql) > 120 else ''}\n")

    res = {
        "key":         source_key,
        "label":       label,
        "sql":         sql,
        "status":      "OK",
        "total_rows":  0,
        "total_cols":  0,
        "schema_rows": [],
        "null_results":[],
        "date_col":    None,
        "date_min":    None,
        "date_max":    None,
        "date_distinct":None,
        "cardinality": {},
        "numeric_stats":[],
        "dup_count":   0,
        "dup_pct":     0.0,
        "score":       0,
        "errors":      [],
        "sample_rows": [],
    }

    # ── Step 1: Execute & Count ───────────────────────────────────────────────
    try:
        df = run_source_sql(sql)
        res["total_rows"] = df.count()
        res["total_cols"] = len(df.columns)
        print(f"  ✅ Step 1 — Executed  |  Rows: {res['total_rows']:,}  |  Cols: {res['total_cols']}")
    except Exception as e:
        res["status"] = "ERROR"
        res["errors"].append(f"SQL execution failed: {str(e)[:200]}")
        print(f"  ❌ Step 1 — SQL FAILED: {str(e)[:200]}")
        results[source_key] = res
        continue

    # ── Step 2: Schema Snapshot ───────────────────────────────────────────────
    print(f"\n  Step 2 — Schema")
    print(f"  {'Column':<45} {'Type':<25} Nullable")
    print("  " + "─" * 75)
    for field in df.schema.fields:
        nullable = "NULL" if field.nullable else "NOT NULL"
        print(f"  {field.name:<45} {str(field.dataType):<25} {nullable}")
        res["schema_rows"].append({
            "name": field.name, "type": str(field.dataType), "nullable": field.nullable
        })

    # ── Step 3: Null Rates ────────────────────────────────────────────────────
    print(f"\n  Step 3 — Null Rates")
    null_flags = []
    for col in df.columns:
        null_count = df.filter(F.col(col).isNull()).count()
        null_pct   = round(null_count / res["total_rows"] * 100, 2) if res["total_rows"] > 0 else 0
        flag = "⚠️  HIGH" if null_pct > 5 else ("🔶 WARN" if null_pct > 1 else "✅  OK")
        null_flags.append(flag)
        if null_pct > 1:  # Only print non-zero to keep output clean
            print(f"    {col:<45} {null_pct:>6.2f}%  {flag}")
        res["null_results"].append({"col": col, "null_count": null_count, "null_pct": null_pct, "flag": flag})
    ok_count = sum(1 for f in null_flags if "OK" in f)
    print(f"  → {ok_count}/{len(df.columns)} columns fully populated")

    # ── Step 4: Date / Year Coverage ─────────────────────────────────────────
    print(f"\n  Step 4 — Temporal Coverage")
    date_col = detect_date_col(df.columns)
    res["date_col"] = date_col
    if date_col:
        try:
            date_stats = df.agg(
                F.min(date_col).alias("min"),
                F.max(date_col).alias("max"),
                F.countDistinct(date_col).alias("distinct")
            ).collect()[0]
            res["date_min"]      = str(date_stats["min"])
            res["date_max"]      = str(date_stats["max"])
            res["date_distinct"] = date_stats["distinct"]
            print(f"  Date column: {date_col}")
            print(f"    Range:    {res['date_min']} → {res['date_max']}")
            print(f"    Distinct: {res['date_distinct']:,} periods")
        except Exception as e:
            print(f"    ⚠️  Could not compute date stats: {str(e)[:100]}")
    else:
        print(f"  ⚠️  No date column detected (looked for anio, fecha, periodo, date, etc.)")

    # ── Step 5: Key Field Cardinality ────────────────────────────────────────
    print(f"\n  Step 5 — Cardinality (join key candidates)")
    id_keywords = ["marca", "brand", "cliente", "customer", "canal", "channel",
                   "sku", "producto", "product", "categoria", "category",
                   "mercado", "market", "region", "zona", "zone"]
    found_keys = [c for c in df.columns if any(k in c.lower() for k in id_keywords)]
    for col in found_keys[:8]:  # max 8 key columns
        try:
            n = df.select(col).distinct().count()
            res["cardinality"][col] = n
            print(f"    {col:<45} {n:>8,} distinct values")
        except:
            pass
    if not found_keys:
        print(f"    ⚠️  No obvious key columns found — review column list manually")

    # ── Step 6: Numeric Volume Stats ─────────────────────────────────────────
    print(f"\n  Step 6 — Numeric Volume Checks")
    num_keywords = ["ventas", "sales", "units", "unidades", "revenue", "ingreso",
                    "inversion", "spend", "monto", "amount", "qty", "cantidad",
                    "waste", "merma", "stock", "inv", "precio", "price"]
    num_cols = [
        f.name for f in df.schema.fields
        if isinstance(f.dataType, NumericType)
        and any(k in f.name.lower() for k in num_keywords)
    ]
    has_numeric = False
    for col in num_cols[:5]:  # max 5 numeric columns
        try:
            stats = df.agg(
                F.min(col).alias("min"), F.max(col).alias("max"),
                F.mean(col).alias("mean"), F.sum(col).alias("total")
            ).collect()[0]
            neg_count = df.filter(F.col(col) < 0).count()
            neg_flag = f"  ⚠️  {neg_count:,} negatives!" if neg_count > 0 else ""
            print(f"    {col:<40} min={stats['min']:>12,.1f}  max={stats['max']:>15,.1f}  total={stats['total']:>18,.0f}{neg_flag}")
            res["numeric_stats"].append({
                "col": col, "min": float(stats["min"] or 0),
                "max": float(stats["max"] or 0), "total": float(stats["total"] or 0),
                "negatives": neg_count
            })
            has_numeric = True
        except:
            pass
    if not num_cols:
        print(f"    ℹ️  No obvious numeric metric columns detected")

    # ── Step 7: Duplicate Check ───────────────────────────────────────────────
    print(f"\n  Step 7 — Duplicate Check")
    natural_key = [date_col] + found_keys[:3] if date_col else found_keys[:4]
    existing_key = [c for c in natural_key if c and c in df.columns]
    if len(existing_key) >= 2:
        deduped    = df.dropDuplicates(existing_key).count()
        dup_count  = res["total_rows"] - deduped
        dup_pct    = round(dup_count / res["total_rows"] * 100, 2) if res["total_rows"] > 0 else 0
        res["dup_count"] = dup_count
        res["dup_pct"]   = dup_pct
        flag = f"⚠️  {dup_count:,} duplicates ({dup_pct}%)" if dup_count > 0 else "✅  No duplicates"
        print(f"    Key: {existing_key}")
        print(f"    {flag}")
    else:
        print(f"    ⚠️  Not enough key columns to check duplicates (need ≥ 2)")

    # ── Step 8: Sample Rows ───────────────────────────────────────────────────
    print(f"\n  Step 8 — Sample (5 rows)")
    sample = df.limit(5).collect()
    res["sample_rows"] = [r.asDict() for r in sample]
    if sample:
        first = sample[0].asDict()
        for k, v in list(first.items())[:10]:
            print(f"    {k:<40} = {v}")
        if len(first) > 10:
            print(f"    ... +{len(first) - 10} more columns")

    # ── Step 9: Readiness Score ───────────────────────────────────────────────
    res["score"] = score_source(
        null_flags = [n["flag"] for n in res["null_results"]],
        has_date   = date_col is not None,
        has_numeric= has_numeric,
        dup_pct    = res["dup_pct"]
    )
    score = res["score"]
    label_score = "🟢 READY" if score >= 80 else ("🟡 CONDITIONAL" if score >= 60 else "🔴 NOT READY")
    print(f"\n  {'─' * 68}")
    print(f"  READINESS SCORE: {score}/100  {label_score}")
    print(f"  {'─' * 68}")

    results[source_key] = res

# COMMAND ----------
# MAGIC %md ## Summary Scorecard

# COMMAND ----------
print("\n" + "═" * 70)
print("SUMMARY SCORECARD")
print("═" * 70)
print(f"  {'Source':<18} {'Domain':<28} {'Rows':>12}  {'Score':>6}  Status")
print("  " + "─" * 70)

for key, res in results.items():
    label_score = "🟢 READY" if res["score"] >= 80 else ("🟡 CONDITIONAL" if res["score"] >= 60 else "🔴 NOT READY")
    print(f"  {key:<18} {res['label']:<28} {res['total_rows']:>12,}  {res['score']:>5}/100  {label_score}")

for key in undefined:
    label = DOMAIN_LABELS.get(key, key)
    print(f"  {key:<18} {label:<28} {'':>12}  {'':>6}  ⏳ TBD — SQL not yet defined")

# COMMAND ----------
# MAGIC %md ## Write Output File

# COMMAND ----------
# ── Build markdown sections ───────────────────────────────────────────────────
source_sections = ""

for key, res in results.items():
    label_score = "🟢 READY" if res["score"] >= 80 else ("🟡 CONDITIONAL" if res["score"] >= 60 else "🔴 NOT READY")

    # Schema table
    schema_md = "\n".join([
        f"| `{r['name']}` | `{r['type']}` | {'✓' if r['nullable'] else '✗'} |"
        for r in res["schema_rows"]
    ])

    # Null rates table
    null_md = "\n".join([
        f"| `{n['col']}` | {n['null_count']:,} | {n['null_pct']}% | {n['flag']} |"
        for n in res["null_results"]
    ])

    # Cardinality table
    card_md = "\n".join([
        f"| `{col}` | {n:,} |"
        for col, n in res["cardinality"].items()
    ]) or "_No key columns auto-detected — fill in manually_"

    # Numeric stats table
    num_md = "\n".join([
        f"| `{s['col']}` | {s['min']:,.1f} | {s['max']:,.1f} | {s['total']:,.0f} | {'⚠️' if s['negatives'] > 0 else '✅'} |"
        for s in res["numeric_stats"]
    ]) or "_No numeric metric columns auto-detected_"

    source_sections += f"""
---

## {key} — {res['label']}

**Readiness Score:** {res['score']}/100 {label_score}
**Rows:** {res['total_rows']:,}  |  **Columns:** {res['total_cols']}
**SQL:**
```sql
{res['sql']}
```

**Date column detected:** `{res['date_col'] or 'NONE — fill in manually'}`
**Date range:** {res['date_min'] or 'N/A'} → {res['date_max'] or 'N/A'} ({res['date_distinct'] or 'N/A'} distinct periods)
**Duplicates:** {res['dup_count']:,} ({res['dup_pct']}%)

### Schema

| Column | Type | Nullable |
|--------|------|---------|
{schema_md}

### Null Rates

| Column | Null Count | Null % | Status |
|--------|-----------|--------|--------|
{null_md}

### Key Field Cardinality

| Column | Distinct Values |
|--------|----------------|
{card_md}

### Numeric Volume Stats

| Column | Min | Max | Total | Negatives? |
|--------|-----|-----|-------|-----------|
{num_md}

### Open Items — Fill in After Reviewing

- [ ] Are all expected columns present?
- [ ] Is the date range correct for this source?
- [ ] Are the key field cardinalities plausible?
- [ ] Are there any negative values that need explanation?
- [ ] What is the natural/business key for this source? (for dedup + joins)
- [ ] Does this source need a JOIN with another view? If so, which one?

"""

# Pending sources section
pending_md = "\n".join([
    f"| `{key}` | {DOMAIN_LABELS.get(key, key)} | SQL not yet defined |"
    for key in undefined
])

# Scorecard table
scorecard_md = "\n".join([
    f"| `{key}` | {res['label']} | {res['total_rows']:,} | {res['score']}/100 | {'🟢 READY' if res['score'] >= 80 else ('🟡 CONDITIONAL' if res['score'] >= 60 else '🔴 NOT READY')} |"
    for key, res in results.items()
])

output_md = f"""# Phase 1 — Source Discovery & Validation

**Generated:** {RUN_AT}
**Snowflake:** `{SF_URL}`
**Warehouse:** `{SF_WAREHOUSE}`
**Sources defined:** {len(defined)}/10
**Sources pending:** {len(undefined)}/10

---

## Scorecard

| Source Key | Domain | Rows | Score | Status |
|------------|--------|------|-------|--------|
{scorecard_md}

---

## Pending Sources (SQL not yet defined)

| Source Key | Domain | Status |
|------------|--------|--------|
{pending_md if pending_md else "_All sources defined ✅_"}

{source_sections}

---

## Next Step

1. Fill in all **Open Items** sections above
2. Add missing SQL definitions to the `SOURCES` dict and re-run for any pending sources
3. Commit and push this file:
   ```
   git add docs/phase_outputs/phase1_data_inventory.md
   git commit -m "data: source discovery and validation — {len(defined)}/10 sources"
   git push origin main
   ```
4. Tell the agent: **"discovery done, inventory committed"**

The agent reads this file and generates:
- Bronze extraction SQL per source
- Data contracts (column types, nullable rules, expected row ranges)
- DQ threshold YAML entries
- Silver transformation logic
"""

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    f.write(output_md)

print(f"\n{'═' * 70}")
print(f"✅ Inventory written to: {OUTPUT_FILE}")
print(f"{'═' * 70}")
print(f"")
print(f"Next steps on your work computer:")
print(f"  git add docs/phase_outputs/phase1_data_inventory.md")
print(f"  git commit -m \"data: source discovery {len(defined)}/10 sources\"")
print(f"  git push origin main")
print(f"")
print(f"Then tell the agent: \"discovery done, inventory committed\"")
