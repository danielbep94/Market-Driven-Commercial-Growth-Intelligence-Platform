# Databricks notebook source
# MAGIC %md
# MAGIC # 00c · Enterprise Catalog Writer
# MAGIC
# MAGIC ## Purpose
# MAGIC This notebook runs **after** `00b_snowflake_discovery.py` and transforms the raw
# MAGIC per-source `results` dict into 10 structured enterprise catalog outputs.
# MAGIC
# MAGIC ## Storage Strategy — No Snowflake DDL Required
# MAGIC All 10 outputs are written to **two destinations** using only permissions you already have:
# MAGIC
# MAGIC | Destination | Format | Location | Requires DDL? |
# MAGIC |-------------|--------|----------|--------------|
# MAGIC | CSV files | CSV | `docs/phase_outputs/` (repo, committed to Git) | ❌ No |
# MAGIC | Delta tables | Delta Lake | Databricks Unity Catalog / DBFS | ❌ No |
# MAGIC
# MAGIC The Delta tables are queryable via Spark SQL in any Databricks notebook and can be
# MAGIC connected directly to Power BI via the Databricks connector — no Snowflake DDL needed.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC Run `00b_snowflake_discovery.py` first (in the same Databricks session, or ensure the
# MAGIC `_discovery_results_cache.json` file was written to `docs/phase_outputs/`).
# MAGIC
# MAGIC ## Outputs Generated
# MAGIC | # | File | Delta Table | Description |
# MAGIC |---|------|-------------|-------------|
# MAGIC | 1 | `phase1_dataset_inventory.csv` | `METADATA.DATASET_INVENTORY` | One row per dataset |
# MAGIC | 2 | `phase1_column_inventory.csv` | `METADATA.COLUMN_INVENTORY` | One row per column |
# MAGIC | 3 | `phase1_relationship_matrix.csv` | `METADATA.RELATIONSHIP_MATRIX` | Cross-dataset column overlap |
# MAGIC | 4 | `business_domain_profile.csv` | `METADATA.BUSINESS_DOMAIN_PROFILE` | Value frequency tables |
# MAGIC | 5 | `data_quality_assessment.csv` | `METADATA.DATA_QUALITY_ASSESSMENT` | DQ rule results |
# MAGIC | 6 | `grain_assessment.csv` | `METADATA.GRAIN_ASSESSMENT` | Grain candidates + uniqueness |
# MAGIC | 7 | `dimension_candidates.csv` | `METADATA.DIMENSION_CANDIDATES` | Columns grouped by dimension |
# MAGIC | 8 | `join_validation_report.csv` | `METADATA.JOIN_VALIDATION_REPORT` | Actual join match rates |
# MAGIC | 9 | `business_glossary.csv` | `METADATA.BUSINESS_GLOSSARY` | Column definitions |
# MAGIC | 10 | `enterprise_readiness_scorecard.csv` | `METADATA.ENTERPRISE_READINESS_SCORECARD` | 6-dim score |

# COMMAND ----------

# MAGIC %md ## ─── SECTION A: CONFIGURATION ────────────────────────────────────────────

# COMMAND ----------

import json, csv, os, yaml
from datetime import datetime
from collections import defaultdict
import pyspark.sql.functions as F
from pyspark.sql.types import StringType

OUTPUT_DIR = "docs/phase_outputs"
RUN_AT     = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Delta Lake persistence target ────────────────────────────────────────────
# Options (choose ONE — comment out the other):
#
# Option A: Unity Catalog (recommended if your workspace has Unity Catalog enabled)
#   Format: <catalog>.<schema>.<table>
#   No DDL needed — tables are created automatically on first write.
#
# Option B: DBFS + Hive metastore (works in any Databricks workspace)
#   Format: <database>.<table>  (stored under dbfs:/user/hive/warehouse/)
#   No DDL needed — database is created automatically.
#
# Uncomment the option that matches your workspace setup:

# OPTION A — Unity Catalog (preferred)
# DELTA_CATALOG = "mgi_dev"    # your Unity Catalog catalog name
# DELTA_SCHEMA  = "metadata"

# OPTION B — DBFS / Hive metastore (fallback)
DELTA_CATALOG = None           # set to None to use Hive metastore
DELTA_SCHEMA  = "mgi_metadata" # Hive database name — created automatically if needed

def _full_table_name(table: str) -> str:
    if DELTA_CATALOG:
        return f"{DELTA_CATALOG}.{DELTA_SCHEMA}.{table.lower()}"
    return f"{DELTA_SCHEMA}.{table.lower()}"

def ensure_delta_schema():
    """Create the Delta schema/database if it does not already exist."""
    if DELTA_CATALOG:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {DELTA_CATALOG}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DELTA_CATALOG}.{DELTA_SCHEMA}")
    else:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {DELTA_SCHEMA}")
    print(f"✅ Delta schema ready: {_full_table_name('')}")

try:
    ensure_delta_schema()
    DELTA_AVAILABLE = True
except Exception as e:
    print(f"⚠️  Could not create Delta schema: {e}")
    print(f"   → CSV outputs only. Check cluster permissions for Hive/Unity Catalog.")
    DELTA_AVAILABLE = False

# COMMAND ----------

# MAGIC %md ## ─── SECTION B: LOAD RESULTS FROM 00b ─────────────────────────────────

# COMMAND ----------

# ── Attempt to use in-memory `results` from the same session first ────────────
try:
    _ = results  # set by 00b in the same session
    print(f"✅ Using in-memory results from 00b — {len(results)} source(s) loaded")
except NameError:
    _cache_path = f"{OUTPUT_DIR}/_discovery_results_cache.json"
    try:
        with open(_cache_path, "r", encoding="utf-8") as f:
            results = json.load(f)
        print(f"✅ Loaded results from cache: {_cache_path}  ({len(results)} source(s))")
    except Exception as e:
        raise RuntimeError(
            f"Could not load discovery results. Run 00b first.\nError: {e}"
        )

# ── Load glossary seed ────────────────────────────────────────────────────────
GLOSSARY_SEED = {}
try:
    with open("configs/business_glossary_seed.yaml", "r") as f:
        seed_data = yaml.safe_load(f) or {}
    for entry in seed_data.get("columns", []):
        GLOSSARY_SEED[entry["column_name"].upper()] = entry
    print(f"✅ Glossary seed loaded — {len(GLOSSARY_SEED)} definitions")
except Exception as e:
    print(f"⚠️  Glossary seed not loaded: {e}")

# COMMAND ----------

# MAGIC %md ## ─── SECTION C: OUTPUT HELPERS ──────────────────────────────────────────

# COMMAND ----------

DOMAIN_LABELS = {
    "DATA_MKT": "Investment / Marketing", "DATA_SELL_IN": "Sell-In",
    "DATA_SELL_OUT": "Sell-Out", "DATA_WASTE": "Waste / Merma",
    "DATA_FORECAST": "Demand Forecast", "DATA_NIELSEN": "Nielsen / Market Share",
    "DATA_PRICE": "Price", "DATA_PROMO": "Promotions",
    "DATA_INVENTORY": "Inventory / Stock", "DATA_CALENDAR": "Calendar / Date Dimension",
}

def score_label(s):
    return "ENTERPRISE READY" if s >= 80 else ("CONDITIONAL" if s >= 60 else "NOT READY")

_written_files = []

def write_csv_and_delta(rows: list, fieldnames: list, csv_filename: str, delta_table: str):
    """
    1. Write rows to CSV in docs/phase_outputs/ (committed to Git).
    2. Write rows as a Delta table in Databricks (queryable via Spark SQL + Power BI).
    No Snowflake DDL permissions required for either destination.
    """
    if not rows:
        print(f"  ⚠️  {csv_filename} — 0 rows, skipped")
        return

    # ── CSV ───────────────────────────────────────────────────────────────────
    csv_path = f"{OUTPUT_DIR}/{csv_filename}"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    # ── Delta Table ───────────────────────────────────────────────────────────
    full_name = _full_table_name(delta_table)
    delta_status = "⏭️  skipped"
    if DELTA_AVAILABLE:
        try:
            # Cast all columns to string for catalog compatibility
            sdf = spark.createDataFrame(rows)
            for col in sdf.columns:
                sdf = sdf.withColumn(col, sdf[col].cast(StringType()))
            sdf = sdf.withColumn("_run_at", F.lit(RUN_AT))
            sdf.write.format("delta").mode("overwrite").saveAsTable(full_name)
            delta_status = f"✅ {full_name}"
        except Exception as e:
            delta_status = f"❌ {str(e)[:120]}"

    print(f"  CSV:   {csv_path}  ({len(rows):,} rows)")
    print(f"  Delta: {delta_status}")
    _written_files.append((csv_filename, delta_table, len(rows), delta_status))

# COMMAND ----------

# MAGIC %md ## Output 1 — Dataset Inventory

# COMMAND ----------

print("=" * 60)
print("OUTPUT 1 — Dataset Inventory")
print("=" * 60)

dataset_rows = []
for key, res in results.items():
    best_grain = next((g for g in res.get("grain_candidates", []) if g.get("is_best")), {})
    dataset_rows.append({
        "run_at":                RUN_AT,
        "dataset_name":          key,
        "domain":                DOMAIN_LABELS.get(key, key),
        "database":              res.get("db", ""),
        "schema":                res.get("schema", ""),
        "status":                res.get("status", ""),
        "row_count":             str(res.get("total_rows", 0)),
        "col_count":             str(res.get("total_cols", 0)),
        "date_column":           res.get("date_col") or "",
        "date_min":              res.get("date_min") or "",
        "date_max":              res.get("date_max") or "",
        "date_distinct_periods": str(res.get("date_distinct") or ""),
        "temporal_gaps":         str(res.get("temporal_gaps", 0)),
        "grain_hint":            res.get("grain_hint", ""),
        "best_detected_grain":   best_grain.get("candidate_grain", ""),
        "grain_uniqueness_pct":  str(best_grain.get("uniqueness_pct", "")),
        "dup_count":             str(res.get("dup_count", 0)),
        "dup_pct":               str(res.get("dup_pct", 0.0)),
        "dq_rules_pass":         str(res.get("dq_pass_count", 0)),
        "dq_rules_fail":         str(res.get("dq_fail_count", 0)),
        "score_completeness":    str(res.get("score_completeness", 0)),
        "score_consistency":     str(res.get("score_consistency", 0)),
        "score_joinability":     str(res.get("score_joinability", 0)),
        "score_temporal":        str(res.get("score_temporal", 0)),
        "score_documentation":   str(res.get("score_documentation", 0)),
        "score_grain":           str(res.get("score_grain", 0)),
        "enterprise_score":      str(res.get("enterprise_score", 0)),
        "readiness_status":      score_label(res.get("enterprise_score", 0)),
    })

FIELDS_DATASET = list(dataset_rows[0].keys()) if dataset_rows else []
write_csv_and_delta(dataset_rows, FIELDS_DATASET, "phase1_dataset_inventory.csv", "DATASET_INVENTORY")

# COMMAND ----------

# MAGIC %md ## Output 2 — Column Inventory

# COMMAND ----------

print("=" * 60)
print("OUTPUT 2 — Column Inventory")
print("=" * 60)

all_col_rows = []
for key, res in results.items():
    for col in res.get("col_inventory", []):
        gl = GLOSSARY_SEED.get(col["column"].upper(), {})
        all_col_rows.append({
            "run_at":               RUN_AT,
            "dataset":              key,
            "column":               col["column"],
            "pyspark_type":         col.get("pyspark_type", ""),
            "sf_type":              col.get("sf_type", ""),
            "nullable":             str(col.get("nullable", "")),
            "null_count":           str(col.get("null_count", 0)),
            "null_pct":             str(col.get("null_pct", 0)),
            "null_flag":            col.get("null_flag", ""),
            "distinct_count":       str(col.get("distinct_count", 0)),
            "is_pk_candidate":      str(col.get("is_pk_candidate", False)),
            "dimension_class":      col.get("dimension_class", ""),
            "in_glossary":          str(col.get("in_glossary", False)),
            "is_business_key":      str(gl.get("is_business_key", False)),
            "business_name":        gl.get("business_name", ""),
            "business_definition":  str(gl.get("definition", "")).replace("\n", " ").strip(),
            "unit":                 gl.get("unit", ""),
            "owner":                gl.get("owner", ""),
            "sample_values":        col.get("sample_values", ""),
        })

FIELDS_COL = [
    "run_at", "dataset", "column", "pyspark_type", "sf_type", "nullable",
    "null_count", "null_pct", "null_flag", "distinct_count", "is_pk_candidate",
    "dimension_class", "in_glossary", "is_business_key", "business_name",
    "business_definition", "unit", "owner", "sample_values",
]
write_csv_and_delta(all_col_rows, FIELDS_COL, "phase1_column_inventory.csv", "COLUMN_INVENTORY")

# COMMAND ----------

# MAGIC %md ## Output 3 — Cross-Dataset Relationship Matrix

# COMMAND ----------

print("=" * 60)
print("OUTPUT 3 — Cross-Dataset Relationship Matrix")
print("=" * 60)

col_to_datasets = defaultdict(list)
for key, res in results.items():
    for col_entry in res.get("col_inventory", []):
        col_norm = col_entry["column"].lower().strip()
        col_to_datasets[col_norm].append({
            "dataset":        key,
            "original_name":  col_entry["column"],
            "dimension_class": col_entry.get("dimension_class", ""),
            "null_pct":       col_entry.get("null_pct", 0),
            "distinct_count": col_entry.get("distinct_count", 0),
        })

# Known cross-dataset alias pairs requiring transforms
ALIAS_TRANSFORMS = {
    frozenset({"fecha", "day_id"}):  "DATE_TRANSFORM",
    frozenset({"anio", "year_id"}):  "DATE_TRANSFORM",
    frozenset({"cadena", "chain"}):  "BUSINESS_MAPPING",
    frozenset({"sku", "cbu_code"}):  "BUSINESS_MAPPING",
    frozenset({"int_id", "upc"}):    "BUSINESS_MAPPING",
}

rel_rows = []
for col_norm, ds_list in col_to_datasets.items():
    if len(ds_list) < 2:
        continue
    for i, ds_a in enumerate(ds_list):
        for ds_b in ds_list[i+1:]:
            match_type = "DIRECT"
            for alias_set, alias_type in ALIAS_TRANSFORMS.items():
                if col_norm in alias_set:
                    match_type = alias_type
                    break
            gl = GLOSSARY_SEED.get(col_norm.upper(), {})
            join_rec = (
                "✅ JOIN READY"          if match_type == "DIRECT"
                else ("⚠️ TRANSFORM NEEDED"  if match_type == "DATE_TRANSFORM"
                else "⚠️ HOMOLOGATION NEEDED")
            )
            rel_rows.append({
                "run_at":            RUN_AT,
                "column_normalized": col_norm,
                "column_in_a":       ds_a["original_name"],
                "column_in_b":       ds_b["original_name"],
                "dataset_a":         ds_a["dataset"],
                "dataset_b":         ds_b["dataset"],
                "dimension_class":   ds_a["dimension_class"],
                "match_type":        match_type,
                "null_pct_a":        str(ds_a["null_pct"]),
                "null_pct_b":        str(ds_b["null_pct"]),
                "distinct_a":        str(ds_a["distinct_count"]),
                "distinct_b":        str(ds_b["distinct_count"]),
                "business_name":     gl.get("business_name", ""),
                "join_recommendation": join_rec,
            })

write_csv_and_delta(
    rel_rows,
    ["run_at", "column_normalized", "column_in_a", "column_in_b", "dataset_a", "dataset_b",
     "dimension_class", "match_type", "null_pct_a", "null_pct_b", "distinct_a", "distinct_b",
     "business_name", "join_recommendation"],
    "phase1_relationship_matrix.csv", "RELATIONSHIP_MATRIX"
)

# COMMAND ----------

# MAGIC %md ## Output 4 — Business Domain Profile

# COMMAND ----------

print("=" * 60)
print("OUTPUT 4 — Business Domain Profile")
print("=" * 60)

domain_rows = []
for key, res in results.items():
    for entry in res.get("domain_profile", []):
        domain_rows.append({
            "run_at":       RUN_AT,
            "dataset":      entry["dataset"],
            "column":       entry["column"],
            "value":        str(entry["value"]),
            "frequency":    str(entry["frequency"]),
            "coverage_pct": str(entry["coverage_pct"]),
        })

write_csv_and_delta(
    domain_rows,
    ["run_at", "dataset", "column", "value", "frequency", "coverage_pct"],
    "business_domain_profile.csv", "BUSINESS_DOMAIN_PROFILE"
)

# COMMAND ----------

# MAGIC %md ## Output 5 — Data Quality Assessment

# COMMAND ----------

print("=" * 60)
print("OUTPUT 5 — Data Quality Assessment")
print("=" * 60)

dq_rows = []
for key, res in results.items():
    for entry in res.get("dq_results", []):
        dq_rows.append({
            "run_at":    RUN_AT,
            "dataset":   entry["dataset"],
            "column":    entry["column"],
            "rule_type": entry["rule_type"],
            "severity":  entry["severity"],
            "expected":  entry["expected"],
            "actual":    entry["actual"],
            "status":    entry["status"],
            "passed":    str(entry["passed"]),
        })

write_csv_and_delta(
    dq_rows,
    ["run_at", "dataset", "column", "rule_type", "severity", "expected", "actual", "status", "passed"],
    "data_quality_assessment.csv", "DATA_QUALITY_ASSESSMENT"
)

# COMMAND ----------

# MAGIC %md ## Output 6 — Grain Assessment

# COMMAND ----------

print("=" * 60)
print("OUTPUT 6 — Grain Assessment")
print("=" * 60)

grain_rows = []
for key, res in results.items():
    for g in res.get("grain_candidates", []):
        grain_rows.append({
            "run_at":          RUN_AT,
            "dataset":         g["dataset"],
            "candidate_grain": g["candidate_grain"],
            "columns_used":    ", ".join(g["columns_used"]),
            "combo_size":      str(g["combo_size"]),
            "uniqueness_pct":  str(g["uniqueness_pct"]),
            "confidence":      g["confidence"],
            "is_best":         str(g["is_best"]),
        })

write_csv_and_delta(
    grain_rows,
    ["run_at", "dataset", "candidate_grain", "columns_used", "combo_size",
     "uniqueness_pct", "confidence", "is_best"],
    "grain_assessment.csv", "GRAIN_ASSESSMENT"
)

# COMMAND ----------

# MAGIC %md ## Output 7 — Dimension Candidates

# COMMAND ----------

print("=" * 60)
print("OUTPUT 7 — Dimension Candidates")
print("=" * 60)

DIM_TAXONOMY = {
    "DIM_PRODUCT":   {"upc", "sku", "int_id", "cbu_code", "descripcion", "marca",
                      "product_id", "cod_producto", "ean", "gtin"},
    "DIM_CUSTOMER":  {"cadena", "chain", "subchain", "format", "canal", "customer_id",
                      "cliente", "retailer", "store_id", "tienda"},
    "DIM_DATE":      {"fecha", "fecha_proceso", "fecha_venta", "fecha_cierre",
                      "day_id", "anio", "mes", "year_id", "dt", "date", "week",
                      "semana", "periodo", "period", "year", "month"},
    "DIM_BRAND":     {"marca", "brand", "categoria", "subcategoria", "category",
                      "subcategory", "negocio", "business"},
    "DIM_MARKETING": {"campana", "campaign", "medio", "media", "plataforma",
                      "platform", "objetivo", "objective", "formato"},
}

dim_rows = []
seen = defaultdict(set)
for key, res in results.items():
    for col_entry in res.get("col_inventory", []):
        col_norm = col_entry["column"].lower()
        for dim, keywords in DIM_TAXONOMY.items():
            if col_norm in keywords:
                dedup_key = (dim, col_norm, key)
                if dedup_key not in seen:
                    seen[dedup_key].add(dedup_key)
                    gl = GLOSSARY_SEED.get(col_entry["column"].upper(), {})
                    dim_rows.append({
                        "run_at":             RUN_AT,
                        "target_dimension":   dim,
                        "column_normalized":  col_norm,
                        "column_original":    col_entry["column"],
                        "dataset":            key,
                        "sf_type":            col_entry.get("sf_type", ""),
                        "null_pct":           str(col_entry.get("null_pct", 0)),
                        "distinct_count":     str(col_entry.get("distinct_count", 0)),
                        "is_pk_candidate":    str(col_entry.get("is_pk_candidate", False)),
                        "business_name":      gl.get("business_name", ""),
                        "definition":         str(gl.get("definition", "")).replace("\n", " ").strip(),
                        "is_business_key":    str(gl.get("is_business_key", False)),
                        "suggested_role":     (
                            "PRIMARY KEY"  if col_entry.get("is_pk_candidate")
                            else ("FOREIGN KEY" if gl.get("is_business_key")
                            else "ATTRIBUTE")
                        ),
                    })

write_csv_and_delta(
    dim_rows,
    ["run_at", "target_dimension", "column_normalized", "column_original", "dataset",
     "sf_type", "null_pct", "distinct_count", "is_pk_candidate", "business_name",
     "definition", "is_business_key", "suggested_role"],
    "dimension_candidates.csv", "DIMENSION_CANDIDATES"
)

# COMMAND ----------

# MAGIC %md ## Output 8 — Join Validation Report

# COMMAND ----------

print("=" * 60)
print("OUTPUT 8 — Join Validation Report")
print("=" * 60)
print("  (Computes actual cross-dataset match rates for shared business keys)")

JOIN_KEY_PRIORITY = [
    "upc", "sku", "marca", "cadena", "chain", "canal",
    "int_id", "cbu_code", "campana", "categoria",
]

join_rows = []

# Attempt to access SOURCES config for live join queries
try:
    _sources = SOURCES  # available if running in same session as 00b
except NameError:
    _sources = {}
    print("  ℹ️  SOURCES not in scope — join validation will use cardinality estimates.")
    print("      For live match rates, run 00b and 00c in the same Databricks session.")

def _get_sf_opts_for_join(cfg: dict) -> dict:
    """Build connector opts for a join validation read. Credentials from scope."""
    try:
        _u = dbutils.secrets.get(scope="DAN-AM-P-KVT800-R-MDP-DB", key="snowflake-user")
        _p = dbutils.secrets.get(scope="DAN-AM-P-KVT800-R-MDP-DB", key="snowflake-password")
    except Exception:
        _u, _p = "MOCK", "MOCK"
    return {
        "sfURL": "danonenam.east-us-2.azure.snowflakecomputing.com",
        "sfUser": _u, "sfPassword": _p,
        "sfDatabase": cfg["db"], "sfSchema": cfg["schema"],
        "sfWarehouse": "PRD_MDP_ANL_WH",
        "sfRole": "PRD_MDP",
    }

for col_norm in JOIN_KEY_PRIORITY:
    # Find all datasets that have this column
    ds_with_col = []
    for key, res in results.items():
        match = next((c for c in res.get("col_inventory", [])
                      if c["column"].lower() == col_norm), None)
        if match:
            ds_with_col.append((key, match["column"]))

    if len(ds_with_col) < 2:
        continue

    print(f"\n  Join key: '{col_norm.upper()}' — found in {len(ds_with_col)} dataset(s): "
          f"{[k for k, _ in ds_with_col]}")

    for i, (key_a, col_a) in enumerate(ds_with_col):
        for key_b, col_b in ds_with_col[i+1:]:
            cfg_a = _sources.get(key_a) if isinstance(_sources.get(key_a), dict) else None
            cfg_b = _sources.get(key_b) if isinstance(_sources.get(key_b), dict) else None

            try:
                if cfg_a and cfg_b:
                    # Live validation via Snowflake reads
                    opts_a = _get_sf_opts_for_join(cfg_a)
                    opts_b = _get_sf_opts_for_join(cfg_b)

                    df_a = (spark.read.format("snowflake").options(**opts_a)
                                  .option("query", f"SELECT DISTINCT {col_a} "
                                                   f"FROM ({str(cfg_a['sql']).strip()}) t "
                                                   f"WHERE {col_a} IS NOT NULL")
                                  .load())
                    df_b = (spark.read.format("snowflake").options(**opts_b)
                                  .option("query", f"SELECT DISTINCT {col_b} "
                                                   f"FROM ({str(cfg_b['sql']).strip()}) t "
                                                   f"WHERE {col_b} IS NOT NULL")
                                  .load())

                    left_count  = df_a.count()
                    matched     = df_a.join(df_b, df_a[col_a] == df_b[col_b], "inner").count()
                    match_rate  = round(matched / left_count * 100, 2) if left_count > 0 else 0.0
                    unmatched   = left_count - matched
                    unmatch_pct = round(100 - match_rate, 2)
                    status      = "✅ PASS" if unmatch_pct <= 20 else "❌ FAIL"
                    method      = "LIVE"
                    print(f"    {key_a}.{col_a} → {key_b}.{col_b}: {match_rate:.1f}% match  {status}")

                else:
                    # Estimate from cardinality when live data unavailable
                    res_a = results[key_a]
                    res_b = results[key_b]
                    d_a = next((c["distinct_count"] for c in res_a.get("col_inventory", [])
                                if c["column"].lower() == col_norm), 0)
                    d_b = next((c["distinct_count"] for c in res_b.get("col_inventory", [])
                                if c["column"].lower() == col_norm), 0)
                    left_count  = d_a or 0
                    matched     = min(d_a or 0, d_b or 0)
                    match_rate  = round(matched / left_count * 100, 2) if left_count > 0 else 0.0
                    unmatch_pct = round(100 - match_rate, 2)
                    status      = "⚠️ ESTIMATED"
                    method      = "CARDINALITY_ESTIMATE"
                    unmatched   = left_count - matched
                    print(f"    {key_a}.{col_a} → {key_b}.{col_b}: ~{match_rate:.0f}% (estimated)")

                join_rows.append({
                    "run_at":         RUN_AT,
                    "join_key":       col_norm.upper(),
                    "dataset_a":      key_a,
                    "column_a":       col_a,
                    "dataset_b":      key_b,
                    "column_b":       col_b,
                    "left_count":     str(left_count),
                    "matched_count":  str(matched),
                    "match_rate_pct": str(match_rate),
                    "unmatched_pct":  str(unmatch_pct),
                    "status":         status,
                    "method":         method,
                })

            except Exception as e:
                print(f"    ⚠️  Join check failed ({key_a}↔{key_b}): {str(e)[:150]}")
                join_rows.append({
                    "run_at": RUN_AT, "join_key": col_norm.upper(),
                    "dataset_a": key_a, "column_a": col_a,
                    "dataset_b": key_b, "column_b": col_b,
                    "left_count": "0", "matched_count": "0",
                    "match_rate_pct": "0", "unmatched_pct": "100",
                    "status": f"ERROR: {str(e)[:100]}", "method": "ERROR",
                })

write_csv_and_delta(
    join_rows,
    ["run_at", "join_key", "dataset_a", "column_a", "dataset_b", "column_b",
     "left_count", "matched_count", "match_rate_pct", "unmatched_pct", "status", "method"],
    "join_validation_report.csv", "JOIN_VALIDATION_REPORT"
)

# COMMAND ----------

# MAGIC %md ## Output 9 — Business Glossary

# COMMAND ----------

print("=" * 60)
print("OUTPUT 9 — Business Glossary")
print("=" * 60)

all_cols_seen = {}
for key, res in results.items():
    for col_entry in res.get("col_inventory", []):
        col_upper = col_entry["column"].upper()
        if col_upper not in all_cols_seen:
            all_cols_seen[col_upper] = {
                "column_name":    col_entry["column"],
                "datasets":       [],
                "sf_type":        col_entry.get("sf_type", ""),
                "null_pct_avg":   col_entry.get("null_pct", 0),
                "distinct_count": col_entry.get("distinct_count", 0),
            }
        all_cols_seen[col_upper]["datasets"].append(key)

glossary_rows = []
documented = 0
undocumented = 0

for col_upper, col_meta in all_cols_seen.items():
    seed = GLOSSARY_SEED.get(col_upper, {})
    has_def = bool(seed)
    documented   += 1 if has_def else 0
    undocumented += 0 if has_def else 1

    glossary_rows.append({
        "run_at":              RUN_AT,
        "column_name":         col_meta["column_name"],
        "business_name":       seed.get("business_name", "[TO BE DEFINED]"),
        "definition":          (str(seed.get("definition", "")).replace("\n", " ").strip()
                                if seed else "[TO BE DEFINED]"),
        "calculation_logic":   seed.get("calculation_logic", "[TO BE DEFINED]"),
        "unit":                seed.get("unit", "[TO BE DEFINED]"),
        "owner":               seed.get("owner", "[TO BE DEFINED]"),
        "dimension":           seed.get("dimension") or "[TO BE CLASSIFIED]",
        "is_business_key":     str(seed.get("is_business_key", False)),
        "datasets":            ", ".join(sorted(set(col_meta["datasets"]))),
        "sf_type":             col_meta["sf_type"],
        "null_pct_avg":        str(col_meta["null_pct_avg"]),
        "distinct_count":      str(col_meta["distinct_count"]),
        "documentation_status": "DOCUMENTED" if has_def else "TO BE DEFINED",
    })

write_csv_and_delta(
    glossary_rows,
    ["run_at", "column_name", "business_name", "definition", "calculation_logic",
     "unit", "owner", "dimension", "is_business_key", "datasets", "sf_type",
     "null_pct_avg", "distinct_count", "documentation_status"],
    "business_glossary.csv", "BUSINESS_GLOSSARY"
)

print(f"\n  Documented:   {documented} columns")
print(f"  Undocumented: {undocumented} columns — add definitions to configs/business_glossary_seed.yaml")

# COMMAND ----------

# MAGIC %md ## Output 10 — Enterprise Readiness Scorecard

# COMMAND ----------

print("=" * 60)
print("OUTPUT 10 — Enterprise Readiness Scorecard")
print("=" * 60)

# Update joinability scores using any live join results
join_by_dataset = defaultdict(list)
for row in join_rows:
    if row.get("method") == "LIVE":
        try:
            rate = float(row["match_rate_pct"])
            join_by_dataset[row["dataset_a"]].append(rate)
            join_by_dataset[row["dataset_b"]].append(rate)
        except (ValueError, KeyError):
            pass

scorecard_rows = []
for key, res in results.items():
    live_rates = join_by_dataset.get(key, [])
    sc_join = (round(sum(live_rates) / len(live_rates) / 100 * 25)
               if live_rates else res.get("score_joinability", 0))
    total = (res.get("score_completeness", 0) + res.get("score_consistency", 0) +
             sc_join + res.get("score_temporal", 0) +
             res.get("score_documentation", 0) + res.get("score_grain", 0))
    best_grain_str = next((g["candidate_grain"] for g in res.get("grain_candidates", [])
                           if g.get("is_best")), "")
    scorecard_rows.append({
        "run_at":                RUN_AT,
        "dataset":               key,
        "domain":                DOMAIN_LABELS.get(key, key),
        "status":                res.get("status", ""),
        "row_count":             str(res.get("total_rows", 0)),
        "score_completeness":    str(res.get("score_completeness", 0)),
        "score_completeness_max": "20",
        "score_consistency":     str(res.get("score_consistency", 0)),
        "score_consistency_max": "20",
        "score_joinability":     str(sc_join),
        "score_joinability_max": "25",
        "score_temporal":        str(res.get("score_temporal", 0)),
        "score_temporal_max":    "15",
        "score_documentation":   str(res.get("score_documentation", 0)),
        "score_documentation_max": "10",
        "score_grain":           str(res.get("score_grain", 0)),
        "score_grain_max":       "10",
        "enterprise_score":      str(total),
        "enterprise_score_max":  "100",
        "readiness_status":      score_label(total),
        "dq_rules_pass":         str(res.get("dq_pass_count", 0)),
        "dq_rules_fail":         str(res.get("dq_fail_count", 0)),
        "best_grain":            best_grain_str,
        "notes":                 (res["errors"][0][:150] if res.get("errors")
                                  else res.get("grain_hint", "")),
    })

write_csv_and_delta(
    scorecard_rows,
    ["run_at", "dataset", "domain", "status", "row_count",
     "score_completeness", "score_completeness_max",
     "score_consistency", "score_consistency_max",
     "score_joinability", "score_joinability_max",
     "score_temporal", "score_temporal_max",
     "score_documentation", "score_documentation_max",
     "score_grain", "score_grain_max",
     "enterprise_score", "enterprise_score_max",
     "readiness_status", "dq_rules_pass", "dq_rules_fail",
     "best_grain", "notes"],
    "enterprise_readiness_scorecard.csv", "ENTERPRISE_READINESS_SCORECARD"
)

# COMMAND ----------

# MAGIC %md ## Final Summary

# COMMAND ----------

print("\n" + "═" * 72)
print("ENTERPRISE METADATA CATALOG — GENERATION COMPLETE")
print("═" * 72)
print(f"  Run at:          {RUN_AT}")
print(f"  Sources:         {len(results)} profiled")
print(f"  Storage:         CSV (Git) + Databricks Delta Lake")
print(f"  Delta schema:    {_full_table_name('')}")
print(f"  Snowflake DDL:   ❌ not required")
print()
print(f"  {'#':<4} {'Output File':<45} {'Rows':>8}  {'Delta':>6}")
print("  " + "─" * 68)

all_ok = True
for num, (fname, dtable, row_count, dstatus) in enumerate(_written_files, 1):
    path     = f"{OUTPUT_DIR}/{fname}"
    csv_ok   = "✅" if os.path.exists(path) else "❌"
    delta_ok = "✅" if "✅" in str(dstatus) else ("⏭️" if "skip" in str(dstatus) else "❌")
    if csv_ok == "❌":
        all_ok = False
    print(f"  {num:<4} {fname:<45} {row_count:>8,}  {csv_ok} CSV  {delta_ok} Δ")

print()
if DELTA_AVAILABLE:
    print(f"  ── Query Delta tables in any notebook: ──────────────────────────────")
    print(f"  spark.sql(\"SELECT * FROM {_full_table_name('BUSINESS_DOMAIN_PROFILE')}\").display()")
    print(f"  spark.sql(\"SELECT * FROM {_full_table_name('JOIN_VALIDATION_REPORT')}\").display()")
    print(f"  spark.sql(\"SELECT * FROM {_full_table_name('ENTERPRISE_READINESS_SCORECARD')}\").display()")
    print()
    print(f"  ── Power BI: connect via Databricks connector ───────────────────────")
    print(f"  Server:   <your-databricks-host>.azuredatabricks.net")
    print(f"  HTTP Path: <your-cluster-http-path>")
    print(f"  Catalog:  {DELTA_CATALOG or 'hive_metastore'}")
    print(f"  Schema:   {DELTA_SCHEMA}")

print()
print(f"  ── Commit CSV outputs to Git: ──────────────────────────────────────")
print(f"  git add docs/phase_outputs/")
print(f"  git commit -m 'data: enterprise metadata catalog — {len(results)} sources profiled'")
print(f"  git push origin main")
print()
print(f"  {'═' * 70}")
print(f"  {'✅ ALL OUTPUTS COMPLETE' if all_ok else '⚠️  SOME OUTPUTS FAILED — review above'}")
print(f"  {'═' * 70}")

# COMMAND ----------


