# Databricks notebook source
# MAGIC %md
# MAGIC # 00c · Enterprise Catalog Writer
# MAGIC
# MAGIC ## Purpose
# MAGIC This notebook runs **after** `00b_snowflake_discovery.py` and transforms the raw
# MAGIC per-source `results` dict into 10 structured enterprise catalog outputs.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC Run `00b_snowflake_discovery.py` first (in the same Databricks session, or ensure the
# MAGIC `_discovery_results_cache.json` file was written to `docs/phase_outputs/`).
# MAGIC
# MAGIC ## Outputs Generated
# MAGIC | # | File | Description |
# MAGIC |---|------|-------------|
# MAGIC | 1 | `phase1_dataset_inventory.csv` | One row per dataset |
# MAGIC | 2 | `phase1_column_inventory.csv` | One row per column across all datasets |
# MAGIC | 3 | `phase1_relationship_matrix.csv` | Cross-dataset column name overlap |
# MAGIC | 4 | `business_domain_profile.csv` | Value frequency tables for business keys |
# MAGIC | 5 | `data_quality_assessment.csv` | DQ rule results per column |
# MAGIC | 6 | `grain_assessment.csv` | Candidate grain combinations + uniqueness % |
# MAGIC | 7 | `dimension_candidates.csv` | Columns grouped by master dimension |
# MAGIC | 8 | `join_validation_report.csv` | Actual join match rates across dataset pairs |
# MAGIC | 9 | `business_glossary.csv` | Column definitions merged with seed glossary |
# MAGIC | 10 | `enterprise_readiness_scorecard.csv` | 6-dimension score per dataset |
# MAGIC
# MAGIC All 10 files are also persisted to Snowflake `MDP_ANALYTICS.METADATA.*`.

# COMMAND ----------
# MAGIC %md ## ─── SECTION A: LOAD RESULTS FROM 00b ─────────────────────────────────

# COMMAND ----------
import json, csv, os, yaml
from datetime import datetime
from collections import defaultdict
import pyspark.sql.functions as F

OUTPUT_DIR = "docs/phase_outputs"
RUN_AT     = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Attempt to use in-memory `results` from the same session first ────────────
try:
    _ = results  # set by 00b in the same session
    print(f"✅ Using in-memory results from 00b — {len(results)} source(s) loaded")
except NameError:
    # Fallback: load from JSON cache
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

# ── Snowflake persistence helpers ─────────────────────────────────────────────
KEYVAULT_NAME   = "DAN-AM-P-KVT800-R-MDP-DB"
SF_URL          = "danonenam.east-us-2.azure.snowflakecomputing.com"
SF_WAREHOUSE    = "PRD_MDP_ANL_WH"
METADATA_DB     = "MDP_ANALYTICS"
METADATA_SCHEMA = "METADATA"

try:
    user     = dbutils.secrets.get(scope=KEYVAULT_NAME, key="snowflake-user")
    password = dbutils.secrets.get(scope=KEYVAULT_NAME, key="snowflake-password")
    SF_AVAILABLE = True
    print(f"✅ Snowflake credentials loaded — catalog will be persisted to {METADATA_DB}.{METADATA_SCHEMA}")
except Exception:
    user, password = "MOCK", "MOCK"
    SF_AVAILABLE = False
    print("⚠️  Snowflake not available — CSV outputs only (no persistence to Snowflake)")

def persist_to_snowflake(sdf, table_name: str):
    """Write a Spark DataFrame to MDP_ANALYTICS.METADATA.<table_name>."""
    if not SF_AVAILABLE:
        print(f"  ⏭️  Skipped Snowflake write for {table_name} (no credentials)")
        return
    try:
        sf_opts = {
            "sfURL": SF_URL, "sfUser": user, "sfPassword": password,
            "sfDatabase": METADATA_DB, "sfSchema": METADATA_SCHEMA,
            "sfWarehouse": SF_WAREHOUSE,
        }
        sdf.write.format("snowflake") \
           .options(**sf_opts) \
           .option("dbtable", table_name) \
           .mode("overwrite") \
           .save()
        count = sdf.count()
        print(f"  ✅ Persisted {count:,} rows → {METADATA_DB}.{METADATA_SCHEMA}.{table_name}")
    except Exception as e:
        print(f"  ❌ Snowflake write failed for {table_name}: {str(e)[:200]}")

def write_csv_and_persist(rows: list, fieldnames: list, filename: str, sf_table: str):
    """Write rows to CSV and persist to Snowflake."""
    path = f"{OUTPUT_DIR}/{filename}"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    print(f"  ✅ Written: {path}  ({len(rows):,} rows)")

    if rows:
        sdf = spark.createDataFrame(rows)
        persist_to_snowflake(sdf, sf_table)
    return path

# COMMAND ----------
# MAGIC %md ## Output 1 — Dataset Inventory

# COMMAND ----------
print("=" * 60)
print("OUTPUT 1 — Dataset Inventory")
print("=" * 60)

DOMAIN_LABELS = {
    "DATA_MKT": "Investment / Marketing", "DATA_SELL_IN": "Sell-In",
    "DATA_SELL_OUT": "Sell-Out", "DATA_WASTE": "Waste / Merma",
    "DATA_FORECAST": "Demand Forecast", "DATA_NIELSEN": "Nielsen / Market Share",
    "DATA_PRICE": "Price", "DATA_PROMO": "Promotions",
    "DATA_INVENTORY": "Inventory / Stock", "DATA_CALENDAR": "Calendar / Date Dimension",
}

def score_label(s):
    return "ENTERPRISE READY" if s >= 80 else ("CONDITIONAL" if s >= 60 else "NOT READY")

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
        "row_count":             res.get("total_rows", 0),
        "col_count":             res.get("total_cols", 0),
        "date_column":           res.get("date_col", ""),
        "date_min":              res.get("date_min", ""),
        "date_max":              res.get("date_max", ""),
        "date_distinct_periods": res.get("date_distinct", ""),
        "temporal_gaps":         res.get("temporal_gaps", 0),
        "grain_hint":            res.get("grain_hint", ""),
        "best_detected_grain":   best_grain.get("candidate_grain", ""),
        "grain_uniqueness_pct":  best_grain.get("uniqueness_pct", ""),
        "dup_count":             res.get("dup_count", 0),
        "dup_pct":               res.get("dup_pct", 0.0),
        "dq_rules_pass":         res.get("dq_pass_count", 0),
        "dq_rules_fail":         res.get("dq_fail_count", 0),
        "score_completeness":    res.get("score_completeness", 0),
        "score_consistency":     res.get("score_consistency", 0),
        "score_joinability":     res.get("score_joinability", 0),
        "score_temporal":        res.get("score_temporal", 0),
        "score_documentation":   res.get("score_documentation", 0),
        "score_grain":           res.get("score_grain", 0),
        "enterprise_score":      res.get("enterprise_score", 0),
        "readiness_status":      score_label(res.get("enterprise_score", 0)),
    })

write_csv_and_persist(
    dataset_rows,
    list(dataset_rows[0].keys()) if dataset_rows else [],
    "phase1_dataset_inventory.csv",
    "DATASET_INVENTORY"
)

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
        row = dict(col)
        row["run_at"]            = RUN_AT
        row["business_name"]     = gl.get("business_name", "")
        row["business_definition"] = gl.get("definition", "")
        row["unit"]              = gl.get("unit", "")
        row["owner"]             = gl.get("owner", "")
        row["is_business_key"]   = gl.get("is_business_key", False)
        all_col_rows.append(row)

FIELDNAMES_COL = [
    "run_at", "dataset", "column", "pyspark_type", "sf_type", "nullable",
    "null_count", "null_pct", "null_flag", "distinct_count", "is_pk_candidate",
    "dimension_class", "in_glossary", "is_business_key", "business_name",
    "business_definition", "unit", "owner", "sample_values",
]

write_csv_and_persist(all_col_rows, FIELDNAMES_COL, "phase1_column_inventory.csv", "COLUMN_INVENTORY")

# COMMAND ----------
# MAGIC %md ## Output 3 — Relationship Matrix

# COMMAND ----------
print("=" * 60)
print("OUTPUT 3 — Cross-Dataset Relationship Matrix")
print("=" * 60)

# Build column → dataset mapping
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

# Known date transform pairs (different names but same business meaning)
DATE_ALIASES = {frozenset({"fecha", "day_id"}): "DATE_TRANSFORM",
                frozenset({"anio", "year_id"}): "DATE_TRANSFORM",
                frozenset({"cadena", "chain"}):  "BUSINESS_MAPPING"}

rel_rows = []
for col_norm, ds_list in col_to_datasets.items():
    if len(ds_list) < 2:
        continue

    for i, ds_a in enumerate(ds_list):
        for ds_b in ds_list[i+1:]:
            pair_set = frozenset({col_norm})
            # Check for known transform aliases
            match_type = "DIRECT"
            for alias_set, alias_type in DATE_ALIASES.items():
                if col_norm in alias_set:
                    match_type = alias_type
                    break

            gl = GLOSSARY_SEED.get(col_norm.upper(), {})
            rel_rows.append({
                "run_at":            RUN_AT,
                "column_normalized": col_norm,
                "column_in_a":       ds_a["original_name"],
                "column_in_b":       ds_b["original_name"],
                "dataset_a":         ds_a["dataset"],
                "dataset_b":         ds_b["dataset"],
                "dimension_class":   ds_a["dimension_class"],
                "match_type":        match_type,
                "null_pct_a":        ds_a["null_pct"],
                "null_pct_b":        ds_b["null_pct"],
                "distinct_a":        ds_a["distinct_count"],
                "distinct_b":        ds_b["distinct_count"],
                "business_name":     gl.get("business_name", ""),
                "join_recommendation": (
                    "✅ JOIN READY" if match_type == "DIRECT"
                    else ("⚠️  TRANSFORM NEEDED" if match_type == "DATE_TRANSFORM"
                    else "⚠️  HOMOLOGATION NEEDED")
                ),
            })

write_csv_and_persist(
    rel_rows,
    [
        "run_at", "column_normalized", "column_in_a", "column_in_b",
        "dataset_a", "dataset_b", "dimension_class", "match_type",
        "null_pct_a", "null_pct_b", "distinct_a", "distinct_b",
        "business_name", "join_recommendation",
    ],
    "phase1_relationship_matrix.csv",
    "RELATIONSHIP_MATRIX"
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
            "value":        entry["value"],
            "frequency":    entry["frequency"],
            "coverage_pct": entry["coverage_pct"],
        })

write_csv_and_persist(
    domain_rows,
    ["run_at", "dataset", "column", "value", "frequency", "coverage_pct"],
    "business_domain_profile.csv",
    "BUSINESS_DOMAIN_PROFILE"
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
            "passed":    entry["passed"],
        })

write_csv_and_persist(
    dq_rows,
    ["run_at", "dataset", "column", "rule_type", "severity", "expected", "actual", "status", "passed"],
    "data_quality_assessment.csv",
    "DATA_QUALITY_ASSESSMENT"
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
            "combo_size":      g["combo_size"],
            "uniqueness_pct":  g["uniqueness_pct"],
            "confidence":      g["confidence"],
            "is_best":         g["is_best"],
        })

write_csv_and_persist(
    grain_rows,
    ["run_at", "dataset", "candidate_grain", "columns_used", "combo_size",
     "uniqueness_pct", "confidence", "is_best"],
    "grain_assessment.csv",
    "GRAIN_ASSESSMENT"
)

# COMMAND ----------
# MAGIC %md ## Output 7 — Dimension Candidates

# COMMAND ----------
print("=" * 60)
print("OUTPUT 7 — Dimension Candidates")
print("=" * 60)

# Rebuild taxonomy from GLOSSARY_SEED + col_inventory
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
# Aggregate across all datasets — one row per (dimension, column_normalized, dataset)
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
                        "null_pct":           col_entry.get("null_pct", 0),
                        "distinct_count":     col_entry.get("distinct_count", 0),
                        "is_pk_candidate":    col_entry.get("is_pk_candidate", False),
                        "business_name":      gl.get("business_name", ""),
                        "definition":         gl.get("definition", ""),
                        "is_business_key":    gl.get("is_business_key", False),
                        "suggested_role":     (
                            "PRIMARY KEY"   if col_entry.get("is_pk_candidate")
                            else ("FOREIGN KEY" if gl.get("is_business_key")
                            else "ATTRIBUTE")
                        ),
                    })

write_csv_and_persist(
    dim_rows,
    [
        "run_at", "target_dimension", "column_normalized", "column_original",
        "dataset", "sf_type", "null_pct", "distinct_count", "is_pk_candidate",
        "business_name", "definition", "is_business_key", "suggested_role",
    ],
    "dimension_candidates.csv",
    "DIMENSION_CANDIDATES"
)

# COMMAND ----------
# MAGIC %md ## Output 8 — Join Validation Report

# COMMAND ----------
print("=" * 60)
print("OUTPUT 8 — Join Validation Report")
print("=" * 60)
print("  (Computes actual cross-dataset match rates for shared business keys)")

# Business key columns to validate joins for
JOIN_KEY_PRIORITY = [
    "upc", "sku", "marca", "cadena", "chain", "canal",
    "int_id", "cbu_code", "campana", "categoria"
]

join_rows = []

# For each pair of datasets that share a business key column, compute match rate
dataset_keys = list(results.keys())

for col_norm in JOIN_KEY_PRIORITY:
    # Find datasets that have this column
    ds_with_col = []
    for key, res in results.items():
        match = next((c for c in res.get("col_inventory", [])
                      if c["column"].lower() == col_norm), None)
        if match:
            ds_with_col.append((key, match["column"]))

    if len(ds_with_col) < 2:
        continue

    print(f"\n  Validating join key: '{col_norm.upper()}' (appears in {len(ds_with_col)} datasets)")

    for i, (key_a, col_a) in enumerate(ds_with_col):
        for key_b, col_b in ds_with_col[i+1:]:
            res_a = results[key_a]
            res_b = results[key_b]

            # We need the DataFrames — re-read only the join key column to save cost
            try:
                cfg_a = None
                cfg_b = None

                # Try to get source configs (may not be available if loaded from cache)
                try:
                    from __main__ import SOURCES  # in-session
                    cfg_a = SOURCES.get(key_a)
                    cfg_b = SOURCES.get(key_b)
                except Exception:
                    pass

                if cfg_a and cfg_b:
                    def get_sf_opts(db, schema):
                        return {
                            "sfURL": SF_URL, "sfUser": user, "sfPassword": password,
                            "sfDatabase": db, "sfSchema": schema, "sfWarehouse": SF_WAREHOUSE,
                        }
                    df_a = spark.read.format("snowflake") \
                                .options(**get_sf_opts(cfg_a["db"], cfg_a["schema"])) \
                                .option("query", f"SELECT DISTINCT {col_a} FROM ({cfg_a['sql']}) t WHERE {col_a} IS NOT NULL") \
                                .load()
                    df_b = spark.read.format("snowflake") \
                                .options(**get_sf_opts(cfg_b["db"], cfg_b["schema"])) \
                                .option("query", f"SELECT DISTINCT {col_b} FROM ({cfg_b['sql']}) t WHERE {col_b} IS NOT NULL") \
                                .load()

                    left_count   = df_a.count()
                    matched      = df_a.join(df_b, df_a[col_a] == df_b[col_b], "inner").count()
                    match_rate   = round(matched / left_count * 100, 2) if left_count > 0 else 0.0
                    unmatched    = left_count - matched
                    unmatched_pct = round(100 - match_rate, 2)
                    status = "✅ PASS" if unmatched_pct <= 20 else "❌ FAIL"
                    print(f"    {key_a}.{col_a} → {key_b}.{col_b}:  {match_rate:.1f}% match  {status}")
                else:
                    # Estimate from cardinality metadata when live data unavailable
                    distinct_a = next((c["distinct_count"] for c in res_a.get("col_inventory", [])
                                       if c["column"].lower() == col_norm), None)
                    distinct_b = next((c["distinct_count"] for c in res_b.get("col_inventory", [])
                                       if c["column"].lower() == col_norm), None)
                    left_count    = distinct_a or 0
                    matched       = min(distinct_a or 0, distinct_b or 0) if distinct_a and distinct_b else 0
                    match_rate    = round(matched / left_count * 100, 2) if left_count > 0 else 0.0
                    unmatched_pct = round(100 - match_rate, 2)
                    status        = "⚠️  ESTIMATED (no live data)"
                    print(f"    {key_a}.{col_a} → {key_b}.{col_b}:  ~{match_rate:.1f}% (estimated from cardinality)")

                join_rows.append({
                    "run_at":          RUN_AT,
                    "join_key":        col_norm.upper(),
                    "dataset_a":       key_a,
                    "column_a":        col_a,
                    "dataset_b":       key_b,
                    "column_b":        col_b,
                    "left_count":      left_count,
                    "matched_count":   matched,
                    "match_rate_pct":  match_rate,
                    "unmatched_pct":   unmatched_pct,
                    "status":          status,
                    "method":          "LIVE" if cfg_a and cfg_b else "ESTIMATED",
                })

            except Exception as e:
                print(f"    ⚠️  Join check failed for {col_norm} ({key_a}↔{key_b}): {str(e)[:150]}")
                join_rows.append({
                    "run_at": RUN_AT, "join_key": col_norm.upper(),
                    "dataset_a": key_a, "column_a": col_a,
                    "dataset_b": key_b, "column_b": col_b,
                    "left_count": 0, "matched_count": 0,
                    "match_rate_pct": 0, "unmatched_pct": 100,
                    "status": f"ERROR: {str(e)[:100]}", "method": "ERROR",
                })

write_csv_and_persist(
    join_rows,
    [
        "run_at", "join_key", "dataset_a", "column_a", "dataset_b", "column_b",
        "left_count", "matched_count", "match_rate_pct", "unmatched_pct", "status", "method",
    ],
    "join_validation_report.csv",
    "JOIN_VALIDATION_REPORT"
)

# COMMAND ----------
# MAGIC %md ## Output 9 — Business Glossary

# COMMAND ----------
print("=" * 60)
print("OUTPUT 9 — Business Glossary")
print("=" * 60)

# Collect all unique columns discovered across all datasets
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
    if has_def:
        documented += 1
    else:
        undocumented += 1

    glossary_rows.append({
        "run_at":            RUN_AT,
        "column_name":       col_meta["column_name"],
        "business_name":     seed.get("business_name", "[TO BE DEFINED]"),
        "definition":        (str(seed.get("definition", "")).replace("\n", " ").strip()
                              if seed else "[TO BE DEFINED]"),
        "calculation_logic": seed.get("calculation_logic", "[TO BE DEFINED]"),
        "unit":              seed.get("unit", "[TO BE DEFINED]"),
        "owner":             seed.get("owner", "[TO BE DEFINED]"),
        "dimension":         seed.get("dimension", "[TO BE CLASSIFIED]"),
        "is_business_key":   seed.get("is_business_key", False),
        "datasets":          ", ".join(sorted(set(col_meta["datasets"]))),
        "sf_type":           col_meta["sf_type"],
        "null_pct_avg":      col_meta["null_pct_avg"],
        "distinct_count":    col_meta["distinct_count"],
        "documentation_status": "DOCUMENTED" if has_def else "TO BE DEFINED",
    })

write_csv_and_persist(
    glossary_rows,
    [
        "run_at", "column_name", "business_name", "definition", "calculation_logic",
        "unit", "owner", "dimension", "is_business_key", "datasets", "sf_type",
        "null_pct_avg", "distinct_count", "documentation_status",
    ],
    "business_glossary.csv",
    "BUSINESS_GLOSSARY"
)

print(f"\n  Documented columns:   {documented}")
print(f"  Undocumented columns: {undocumented} → add to business_glossary_seed.yaml")

# COMMAND ----------
# MAGIC %md ## Output 10 — Enterprise Readiness Scorecard

# COMMAND ----------
print("=" * 60)
print("OUTPUT 10 — Enterprise Readiness Scorecard")
print("=" * 60)

# Update joinability scores using actual join validation results (if available)
join_by_dataset = defaultdict(list)
for row in join_rows:
    if row["method"] == "LIVE":
        join_by_dataset[row["dataset_a"]].append(row["match_rate_pct"])
        join_by_dataset[row["dataset_b"]].append(row["match_rate_pct"])

scorecard_rows = []
for key, res in results.items():
    # Recalculate joinability if live join data is available
    live_rates = join_by_dataset.get(key, [])
    if live_rates:
        avg_match = sum(live_rates) / len(live_rates)
        sc_join = round(avg_match / 100 * 25)
    else:
        sc_join = res.get("score_joinability", 0)

    total = (res.get("score_completeness", 0) + res.get("score_consistency", 0) +
             sc_join + res.get("score_temporal", 0) +
             res.get("score_documentation", 0) + res.get("score_grain", 0))

    scorecard_rows.append({
        "run_at":               RUN_AT,
        "dataset":              key,
        "domain":               DOMAIN_LABELS.get(key, key),
        "status":               res.get("status", ""),
        "row_count":            res.get("total_rows", 0),
        "score_completeness":   res.get("score_completeness", 0),
        "score_completeness_max": 20,
        "score_consistency":    res.get("score_consistency", 0),
        "score_consistency_max": 20,
        "score_joinability":    sc_join,
        "score_joinability_max": 25,
        "score_temporal":       res.get("score_temporal", 0),
        "score_temporal_max":   15,
        "score_documentation":  res.get("score_documentation", 0),
        "score_documentation_max": 10,
        "score_grain":          res.get("score_grain", 0),
        "score_grain_max":      10,
        "enterprise_score":     total,
        "enterprise_score_max": 100,
        "readiness_status":     score_label(total),
        "dq_rules_pass":        res.get("dq_pass_count", 0),
        "dq_rules_fail":        res.get("dq_fail_count", 0),
        "best_grain":           next((g["candidate_grain"] for g in res.get("grain_candidates", [])
                                      if g.get("is_best")), ""),
        "notes":                (f"{res['errors'][0][:150]}" if res.get("errors")
                                 else res.get("grain_hint", "")),
    })

write_csv_and_persist(
    scorecard_rows,
    [
        "run_at", "dataset", "domain", "status", "row_count",
        "score_completeness", "score_completeness_max",
        "score_consistency", "score_consistency_max",
        "score_joinability", "score_joinability_max",
        "score_temporal", "score_temporal_max",
        "score_documentation", "score_documentation_max",
        "score_grain", "score_grain_max",
        "enterprise_score", "enterprise_score_max",
        "readiness_status", "dq_rules_pass", "dq_rules_fail",
        "best_grain", "notes",
    ],
    "enterprise_readiness_scorecard.csv",
    "ENTERPRISE_READINESS_SCORECARD"
)

# COMMAND ----------
# MAGIC %md ## Final Summary

# COMMAND ----------
print("\n" + "═" * 70)
print("ENTERPRISE METADATA CATALOG — GENERATION COMPLETE")
print("═" * 70)
print(f"  Run at: {RUN_AT}")
print(f"  Sources profiled: {len(results)}")
print(f"\n  {'#':<4} {'Output File':<45} {'Rows':>8}")
print("  " + "─" * 60)

outputs = [
    (1,  "phase1_dataset_inventory.csv",      len(dataset_rows)),
    (2,  "phase1_column_inventory.csv",       len(all_col_rows)),
    (3,  "phase1_relationship_matrix.csv",    len(rel_rows)),
    (4,  "business_domain_profile.csv",       len(domain_rows)),
    (5,  "data_quality_assessment.csv",       len(dq_rows)),
    (6,  "grain_assessment.csv",              len(grain_rows)),
    (7,  "dimension_candidates.csv",          len(dim_rows)),
    (8,  "join_validation_report.csv",        len(join_rows)),
    (9,  "business_glossary.csv",             len(glossary_rows)),
    (10, "enterprise_readiness_scorecard.csv", len(scorecard_rows)),
]

all_ok = True
for num, fname, row_count in outputs:
    path = f"{OUTPUT_DIR}/{fname}"
    exists = os.path.exists(path)
    status = "✅" if exists else "❌"
    if not exists:
        all_ok = False
    print(f"  {num:<4} {fname:<45} {row_count:>8,}  {status}")

if SF_AVAILABLE:
    print(f"\n  Snowflake persistence: ✅ {METADATA_DB}.{METADATA_SCHEMA}")
else:
    print(f"\n  Snowflake persistence: ⏭️  Skipped (no credentials in this env)")

print(f"\n  {'═' * 68}")
if all_ok:
    print(f"  ✅ ALL 10 OUTPUTS WRITTEN SUCCESSFULLY")
    print(f"\n  Next steps:")
    print(f"    git add docs/phase_outputs/")
    print(f"    git commit -m 'data: enterprise metadata catalog — {len(results)} sources profiled'")
    print(f"    git push origin main")
    print(f"    Then: open docs/phase_outputs/ and review business_domain_profile.csv")
    print(f"          and join_validation_report.csv with the business team.")
else:
    print(f"  ⚠️  SOME OUTPUTS FAILED — review errors above")
print(f"  {'═' * 68}")
