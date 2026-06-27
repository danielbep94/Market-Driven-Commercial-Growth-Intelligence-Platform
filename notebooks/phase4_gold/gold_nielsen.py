# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 4 Gold — Nielsen KPIs
# MAGIC **gold_nielsen.py**
# MAGIC
# MAGIC Inputs:
# MAGIC   logs/nielsen_std.csv       (362 rows — market dimension mapping)
# MAGIC   logs/nielsen_facts_std.csv (100,001 rows — fact data, confirmed Phase 3 Silver output)
# MAGIC
# MAGIC Outputs:
# MAGIC   gold_nielsen_kpi.csv        Grain: fecha_month x canal_std x region_std  (detailed)
# MAGIC   gold_nielsen_kpi_master.csv Grain: fecha_month x canal_std               (master-safe)
# MAGIC
# MAGIC F3 resolution:
# MAGIC   nielsen_facts_std.csv confirmed as Phase 3 Silver output (100K rows, 19MB, commit e3bbf81).
# MAGIC
# MAGIC F2 resolution:
# MAGIC   Nielsen measures market-level share — no marca_std in source.
# MAGIC   Master join into gold_commercial_kpi is on (fecha_month, canal_std) only.
# MAGIC   region_std stays in detailed table; NOT included in master grain.
# MAGIC
# MAGIC Key fact columns (from actual data):
# MAGIC   MRKT_DSC_SHRT, hierarchy_level, PERIOD_END_DATE, METRIC_NAME, METRIC_VALUE,
# MAGIC   ROW_COUNT, canal_std, region_std, market_mapping_status, cbu_source

# COMMAND ----------

# MAGIC %run ./gold_kpi_utils

# COMMAND ----------

import os
from pyspark.sql import functions as F

SECTION = "NIELSEN"
log_gold("INFO", "=" * 72, SECTION)
log_gold("INFO", "GOLD_NIELSEN — START", SECTION)

check_run_mode()

# COMMAND ----------

# MAGIC %md ## Step 1 — Load Silver Nielsen inputs

# COMMAND ----------

FACTS_PATH  = os.path.join(LOGS_DIR, "nielsen_facts_std.csv")
MARKET_PATH = os.path.join(LOGS_DIR, "nielsen_std.csv")

# B17 — nielsen_facts_std.csv must exist
gold_blocker("B17", not os.path.exists(FACTS_PATH),
             f"nielsen_facts_std.csv not found at {FACTS_PATH}", SECTION)

log_gold("INFO", f"Loading facts: {FACTS_PATH}", SECTION)
# pandas default quoting handles the """hierarchy_level""" doubled-quote header correctly
df_facts = read_silver_csv(FACTS_PATH)

log_gold("INFO", f"Loading market dim: {MARKET_PATH}", SECTION)
df_market = read_silver_csv(MARKET_PATH)


facts_count  = df_facts.count()
market_count = df_market.count()
log_gold("INFO", f"nielsen_facts_std: {facts_count:,} rows | nielsen_std: {market_count:,} rows", SECTION)

gold_blocker("B1", facts_count == 0, "nielsen_facts_std.csv is empty", SECTION)

# COMMAND ----------

# MAGIC %md ## Step 2 — Discovery: Log all unique METRIC_NAME values (B17)

# COMMAND ----------

metric_names = [row["METRIC_NAME"] for row in df_facts.select("METRIC_NAME").distinct().collect()]
metric_names_sorted = sorted(metric_names)

log_gold("INFO", f"B17 METRIC_NAME discovery: {len(metric_names_sorted)} unique values found:", SECTION)
for m in metric_names_sorted:
    log_gold("INFO", f"  METRIC_NAME = '{m}'", "NIELSEN_DISCOVERY")

gold_passed("B17", f"METRIC_NAME discovery complete — {len(metric_names_sorted)} metrics found", SECTION)

# Known metric mappings (map discovered names to canonical KPI names)
# Will be extended as actual metric names are discovered
METRIC_MAP = {
    # ── Core volume / value metrics ───────────────────────────────────────────
    "U":               "nls_units",              # unit sales
    "E":               "nls_equiv_units",         # equivalent units (volume normalised)
    "DOL":             "nls_dollar_sales",        # dollar/peso sales value

    # ── Price metrics ─────────────────────────────────────────────────────────
    "AVG_U_PRC":       "nls_avg_unit_price",      # average price per unit
    "AVG_E_PRC":       "nls_avg_equiv_price",     # average price per equivalent unit

    # ── Rate-of-sale metrics ─────────────────────────────────────────────────
    "U_ROS":           "nls_units_ros",           # units rate of sale per store per week
    "E_ROS":           "nls_equiv_ros",           # equivalent units rate of sale
    "DOL_ROS":         "nls_dollar_ros",          # dollar rate of sale

    # ── Share metrics ─────────────────────────────────────────────────────────
    "VALUE_SHARE":     "nls_value_share",         # value market share %
    "VOLUME_SHARE":    "nls_volume_share",        # volume market share %

    # ── Distribution metrics ─────────────────────────────────────────────────
    "NUMERIC_DISTRIBUTION": "nls_numeric_dist",  # numeric distribution %
    "NUM_DIST":             "nls_numeric_dist",  # alias used in some Nielsen extracts

    # ── Category metrics ─────────────────────────────────────────────────────
    "CATEGORY_VALUE":  "nls_category_value_mxn", # total category value

    # ── Out-of-stock / context metrics (W4-discovered 2026-06-27) ────────────
    "NUMERIC_DIST_OOSLS":              "nls_numeric_dist_oos",       # numeric dist excl OOS
    "DOL_CCV_REACH_CONTEXT":           "nls_dollar_ccv_reach",       # $ with reach context
    "DOL_CCV_NON_REACH_CONTEXT_OOSLS": "nls_dollar_ccv_oos_no_reach",# $ excl OOS, no reach
    "DOL_CCV_TDP_REACH_CONTEXT":       "nls_dollar_ccv_tdp_reach",   # $ TDP with reach
}


mapped  = [m for m in metric_names_sorted if m in METRIC_MAP]
unmapped = [m for m in metric_names_sorted if m not in METRIC_MAP]

log_gold("INFO", f"Mapped metrics: {mapped}", SECTION)
if unmapped:
    gold_warn("W4", True,
              f"W4: {len(unmapped)} Nielsen METRIC_NAME values not in METRIC_MAP: {unmapped}. "
              f"They will be retained with metric name as column name.", SECTION)

# COMMAND ----------

# MAGIC %md ## Step 3 — Month-Truncate from PERIOD_END_DATE (B13)

# COMMAND ----------

df_facts = df_facts.withColumn(
    "fecha_month",
    month_trunc(F.to_date(F.col("PERIOD_END_DATE"), "yyyy-MM-dd"))
)

null_fecha = df_facts.filter(F.col("fecha_month").isNull()).count()
gold_blocker("B2", null_fecha > 0,
             f"nielsen_facts_std has {null_fecha} NULL fecha_month rows", SECTION)
if null_fecha == 0:
    gold_passed("B2", "fecha_month has zero NULLs in Nielsen facts", SECTION)

check_fecha_month_range(df_facts, "nielsen_facts_std")

# COMMAND ----------

# MAGIC %md ## Step 4 — Pivot METRIC_NAME → columns (Detailed Grain)

# COMMAND ----------

# Pivot on METRIC_NAME: one column per metric, value = SUM(METRIC_VALUE)
# Grain before pivot: fecha_month x MRKT_DSC_SHRT x canal_std x region_std
DETAIL_KEYS = ["fecha_month", "MRKT_DSC_SHRT", "canal_std", "region_std"]

# Get all unique metric names for pivot
all_metrics = metric_names_sorted

df_pivot = (
    df_facts
    .groupBy(*DETAIL_KEYS)
    .pivot("METRIC_NAME", all_metrics)
    .agg(F.sum("METRIC_VALUE"))
)

# Rename pivoted columns to canonical KPI names
for raw_name, canonical_name in METRIC_MAP.items():
    if raw_name in df_pivot.columns:
        df_pivot = df_pivot.withColumnRenamed(raw_name, canonical_name)
    else:
        # Expected metric not found — add as NULL column with warning already raised (W4)
        df_pivot = df_pivot.withColumn(canonical_name, F.lit(None).cast("double"))

detail_count = df_pivot.count()
log_gold("INFO", f"gold_nielsen_kpi (detailed): {detail_count:,} rows at grain {DETAIL_KEYS}", SECTION)

# B8 — no fan-out vs facts input
gold_blocker("B8", detail_count > facts_count,
             f"Nielsen output ({detail_count}) > Silver input ({facts_count}) — fan-out!",
             SECTION)

# COMMAND ----------

# MAGIC %md ## Step 5 — Master-Safe Aggregation (Grain: fecha_month x canal_std)

# COMMAND ----------

MASTER_KEYS = ["fecha_month", "canal_std"]

# For master: SUM numeric KPIs, collapse region_std
numeric_kpi_cols = [c for c in df_pivot.columns if c not in DETAIL_KEYS]

agg_exprs = [F.sum(F.col(c)).alias(c) for c in numeric_kpi_cols if c != "MRKT_DSC_SHRT"]

df_master = (
    df_pivot
    .groupBy(*MASTER_KEYS)
    .agg(*agg_exprs)
)

master_count = df_master.count()
log_gold("INFO", f"gold_nielsen_kpi_master: {master_count:,} rows at grain {MASTER_KEYS}", SECTION)

# B10 — uniqueness on master join keys
assert_unique_keys(df_master, MASTER_KEYS, "gold_nielsen_kpi_master")

# COMMAND ----------

# MAGIC %md ## Step 6 — Coverage Summary

# COMMAND ----------

stats = df_master.agg(
    F.min("fecha_month").alias("min_month"),
    F.max("fecha_month").alias("max_month"),
    F.countDistinct("fecha_month").alias("distinct_months"),
    F.countDistinct("canal_std").alias("distinct_canales"),
).collect()[0]

log_gold("INFO", f"Nielsen fecha_month range: {stats['min_month']} → {stats['max_month']}", SECTION)
log_gold("INFO",
         f"distinct months: {stats['distinct_months']}, distinct canales: {stats['distinct_canales']}",
         SECTION)

# W4 — expected KPIs that are NULL
for expected_kpi in ["nls_value_share", "nls_volume_share", "nls_numeric_dist", "nls_category_value_mxn"]:
    if expected_kpi not in df_master.columns:
        gold_warn("W4", True,
                  f"Expected Nielsen KPI '{expected_kpi}' not found — output as NULL column", SECTION)
        df_master = df_master.withColumn(expected_kpi, F.lit(None).cast("double"))

# COMMAND ----------

# MAGIC %md ## Step 7 — Save Outputs

# COMMAND ----------

save_gold_df(df_pivot,  "gold_nielsen_kpi",        SECTION)
save_gold_df(df_master, "gold_nielsen_kpi_master",  SECTION)

# COMMAND ----------

# MAGIC %md ## Step 8 — Register KPIs

# COMMAND ----------

_METRICS = [
    # Core
    ("nls_units",               "NIELSEN_FACTS", "SUM(METRIC_VALUE) WHERE METRIC='U'",              "Units",    "fecha_month x canal_std x region_std"),
    ("nls_equiv_units",         "NIELSEN_FACTS", "SUM(METRIC_VALUE) WHERE METRIC='E'",              "Eq.Units", "fecha_month x canal_std x region_std"),
    ("nls_dollar_sales",        "NIELSEN_FACTS", "SUM(METRIC_VALUE) WHERE METRIC='DOL'",            "MXN",      "fecha_month x canal_std x region_std"),
    # Price
    ("nls_avg_unit_price",      "NIELSEN_FACTS", "SUM(METRIC_VALUE) WHERE METRIC='AVG_U_PRC'",      "MXN/U",    "fecha_month x canal_std x region_std"),
    ("nls_avg_equiv_price",     "NIELSEN_FACTS", "SUM(METRIC_VALUE) WHERE METRIC='AVG_E_PRC'",      "MXN/Eq",   "fecha_month x canal_std x region_std"),
    # Rate of sale
    ("nls_units_ros",           "NIELSEN_FACTS", "SUM(METRIC_VALUE) WHERE METRIC='U_ROS'",          "U/store/wk","fecha_month x canal_std x region_std"),
    ("nls_equiv_ros",           "NIELSEN_FACTS", "SUM(METRIC_VALUE) WHERE METRIC='E_ROS'",          "Eq/store/wk","fecha_month x canal_std x region_std"),
    ("nls_dollar_ros",          "NIELSEN_FACTS", "SUM(METRIC_VALUE) WHERE METRIC='DOL_ROS'",        "MXN/store/wk","fecha_month x canal_std x region_std"),
    # Share
    ("nls_value_share",         "NIELSEN_FACTS", "SUM(METRIC_VALUE) WHERE METRIC='VALUE_SHARE'",    "%",        "fecha_month x canal_std"),
    ("nls_volume_share",        "NIELSEN_FACTS", "SUM(METRIC_VALUE) WHERE METRIC='VOLUME_SHARE'",   "%",        "fecha_month x canal_std"),
    # Distribution
    ("nls_numeric_dist",        "NIELSEN_FACTS", "SUM(METRIC_VALUE) WHERE METRIC='NUMERIC_DISTRIBUTION|NUM_DIST'", "%", "fecha_month x canal_std"),
    ("nls_numeric_dist_oos",    "NIELSEN_FACTS", "SUM(METRIC_VALUE) WHERE METRIC='NUMERIC_DIST_OOSLS'","%",     "fecha_month x canal_std"),
    # Category
    ("nls_category_value_mxn",  "NIELSEN_FACTS", "SUM(METRIC_VALUE) WHERE METRIC='CATEGORY_VALUE'", "MXN",     "fecha_month x canal_std"),
    # OOS / context (W4-discovered 2026-06-27)
    ("nls_dollar_ccv_reach",      "NIELSEN_FACTS", "SUM WHERE METRIC='DOL_CCV_REACH_CONTEXT'",           "MXN",  "fecha_month x canal_std"),
    ("nls_dollar_ccv_oos_no_reach","NIELSEN_FACTS","SUM WHERE METRIC='DOL_CCV_NON_REACH_CONTEXT_OOSLS'", "MXN",  "fecha_month x canal_std"),
    ("nls_dollar_ccv_tdp_reach",   "NIELSEN_FACTS","SUM WHERE METRIC='DOL_CCV_TDP_REACH_CONTEXT'",       "MXN",  "fecha_month x canal_std"),
]
for m in _METRICS:
    register_gold_metric(*m)


log_gold("INFO", f"Registered {len(_METRICS)} Nielsen KPI metrics", SECTION)
log_gold("INFO", "GOLD_NIELSEN — COMPLETE ✅", SECTION)
write_audit_log()


