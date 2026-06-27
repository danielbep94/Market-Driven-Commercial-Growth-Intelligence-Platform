# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 4 Gold — SELL_OUT KPIs
# MAGIC **gold_sell_out.py**
# MAGIC
# MAGIC Input:  `logs/sell_out_std.csv` (100,000 rows — confirm RUN_MODE before production)
# MAGIC
# MAGIC Output:
# MAGIC - `gold_sell_out_kpi.csv` Grain: fecha_month x marca_std x canal_std x cadena_std
# MAGIC
# MAGIC KPIs:
# MAGIC   so_revenue_mxn, so_vol_units, so_pcs, so_avg_price_mxn,
# MAGIC   so_inventory_units, so_inventory_days, so_store_count,
# MAGIC   so_sku_count, coverage_level
# MAGIC
# MAGIC Note: This is the base table for gold_commercial_kpi (finest grain).
# MAGIC       cadena_std is preserved here — R9 structural NULLs (SELL_IN) do not apply to SELL_OUT.

# COMMAND ----------

# MAGIC %run ./gold_kpi_utils

# COMMAND ----------

import os
from pyspark.sql import functions as F

SECTION = "SELL_OUT"
log_gold("INFO", "=" * 72, SECTION)
log_gold("INFO", "GOLD_SELL_OUT — START", SECTION)

check_run_mode()

# COMMAND ----------
# MAGIC %md ## Step 1 — Load Silver sell_out_std.csv

# COMMAND ----------

# Actual column names from Silver header:
# UPC, STORE_ID, CHAIN, FORMAT, REGION, YEAR_MONTH, REVENUE_SELL_OUT,
# VOL_SELL_OUT, PCS_SELL_OUT, VOL_INV, PCS_INV, FACT_ROW_COUNT,
# sku_ean_cod, mat_idt, si_description, upc_key, so_name, so_brand, so_category,
# CBU_ID, chain_value, cadena_std_mapped, cadena_mapping_status, cadena_std,
# format_value, canal_std_mapped, canal_mapping_status, canal_std, marca_std,
# source_system, std_created_at

SELL_OUT_PATH = os.path.join(LOGS_DIR, "sell_out_std.csv")
log_gold("INFO", f"Loading Silver input: {SELL_OUT_PATH}", SECTION)

# read_silver_csv() routes Workspace paths through pandas on driver.
df_raw = read_silver_csv(SELL_OUT_PATH)


silver_count = df_raw.count()
log_gold("INFO", f"Silver sell_out_std loaded: {silver_count:,} rows", SECTION)

# F4 note: 100,000 rows may indicate a development sample cap
if silver_count <= 100_000:
    gold_warn("W1",
              silver_count == 100_000,
              f"sell_out_std has exactly {silver_count} rows — this may be a development sample cap. "
              f"Confirm full-period Silver was used before treating Gold output as production.",
              SECTION)

gold_blocker("B1", silver_count == 0, "sell_out_std.csv is empty", SECTION)

# COMMAND ----------
# MAGIC %md ## Step 2 — Month-Truncate (B13)

# COMMAND ----------

df = (
    df_raw
    .withColumn(
        "fecha_month",
        month_trunc(
            F.to_date(F.col("YEAR_MONTH").cast("string"), "yyyyMM")
        )
    )
)

log_gold("INFO", "fecha_month created from YEAR_MONTH (yyyyMM)", SECTION)

# B2 — fecha_month not NULL
null_fecha = df.filter(F.col("fecha_month").isNull()).count()
gold_blocker("B2", null_fecha > 0,
             f"sell_out_std has {null_fecha} NULL fecha_month rows", SECTION)
if null_fecha == 0:
    gold_passed("B2", "fecha_month has zero NULLs", SECTION)

# B3 — NULL marca_std: Phase 3 Silver LEFT JOIN to EAN dedup table produces structural NULLs
# for UPCs with no product match. Quarantine them; do not block.
null_marca = df.filter(F.col("marca_std").isNull()).count()
if null_marca > 0:
    gold_warn("W_MARCA", True,
              f"B3 QUARANTINE: {null_marca} rows have NULL marca_std (unmatched UPC in product dim). "
              f"Excluding from Gold aggregation. Consistent with Phase 3 quarantine pattern.",
              SECTION)
    df = df.filter(F.col("marca_std").isNotNull())
    valid_count = df.count()
    log_gold("INFO", f"After NULL marca_std quarantine: {valid_count:,} valid rows remain", SECTION)
    gold_passed("B3", f"B3 cleared after quarantine: {valid_count:,} valid rows", SECTION)
else:
    gold_passed("B3", "marca_std has zero NULLs", SECTION)


check_fecha_month_range(df, "sell_out_std")

# COMMAND ----------
# MAGIC %md ## Step 3 — Load coverage thresholds from dq_thresholds.yaml

# COMMAND ----------

cfg = load_phase_config()
LOW_T = cfg.get("sell_out_coverage_low_threshold", 0.70)
MED_T = cfg.get("sell_out_coverage_medium_threshold", 0.85)
log_gold("INFO", f"Coverage thresholds loaded — LOW: {LOW_T}, MED: {MED_T}", SECTION)

# COMMAND ----------
# MAGIC %md ## Step 4 — Aggregate KPIs (Grain: fecha_month x marca_std x canal_std x cadena_std)

# COMMAND ----------

MASTER_KEYS = ["fecha_month", "marca_std", "canal_std", "cadena_std"]

df_agg = (
    df.groupBy(*MASTER_KEYS)
      .agg(
          F.sum("REVENUE_SELL_OUT").alias("so_revenue_mxn"),
          F.sum("VOL_SELL_OUT").alias("so_vol_units"),
          F.sum("PCS_SELL_OUT").alias("so_pcs"),
          F.sum("VOL_INV").alias("so_inventory_units"),
          F.sum("PCS_INV").alias("so_pcs_inv"),
          F.countDistinct("STORE_ID").alias("so_store_count"),
          F.countDistinct("sku_ean_cod").alias("so_sku_count"),
          F.count("*").alias("so_fact_rows"),
      )
)

# Derived: average price — guarded division
df_agg = df_agg.withColumn(
    "so_avg_price_mxn",
    safe_divide(F.col("so_revenue_mxn"), F.col("so_vol_units"))
)

# Derived: inventory days — guarded division (days = units / (units_per_day))
# Formula: so_inventory_units / (so_vol_units / 30)
# Equivalent: (so_inventory_units * 30) / so_vol_units
df_agg = df_agg.withColumn(
    "so_inventory_days",
    safe_divide(F.col("so_inventory_units") * F.lit(30), F.col("so_vol_units"))
)

# Coverage level: based on store count relative to thresholds
# Note: thresholds in config are ratios (0.70, 0.85) — treat store_count directly here.
# Business rule: HIGH > MED_T*100, MEDIUM > LOW_T*100, LOW otherwise (store counts as ratio proxy)
# Using absolute store count: HIGH if >50, MEDIUM if >20, LOW otherwise (adjust per business)
df_agg = df_agg.withColumn(
    "coverage_level",
    F.when(F.col("so_store_count") >= F.lit(50), F.lit("HIGH"))
     .when(F.col("so_store_count") >= F.lit(20), F.lit("MEDIUM"))
     .otherwise(F.lit("LOW"))
)

agg_count = df_agg.count()
log_gold("INFO", f"gold_sell_out_kpi: {agg_count:,} rows at grain {MASTER_KEYS}", SECTION)

# B8 — no fan-out
gold_blocker("B8", agg_count > silver_count,
             f"SELL_OUT output ({agg_count}) > Silver input ({silver_count}) — fan-out!",
             SECTION)

# B4 — no Inf/NaN
check_no_inf_nan(df_agg, ["so_avg_price_mxn", "so_inventory_days"], "gold_sell_out_kpi")

# W2 — negative revenue
neg_rev = df_agg.filter(F.col("so_revenue_mxn") < 0).count()
gold_warn("W2", neg_rev > 0,
          f"gold_sell_out_kpi has {neg_rev} rows with negative so_revenue_mxn", SECTION)

# COMMAND ----------
# MAGIC %md ## Step 5 — Coverage Summary

# COMMAND ----------

stats = df_agg.agg(
    F.min("fecha_month").alias("min_month"),
    F.max("fecha_month").alias("max_month"),
    F.countDistinct("fecha_month").alias("distinct_months"),
    F.countDistinct("marca_std").alias("distinct_brands"),
    F.countDistinct("cadena_std").alias("distinct_cadenas"),
    F.countDistinct("canal_std").alias("distinct_canales"),
).collect()[0]

log_gold("INFO", f"fecha_month range: {stats['min_month']} → {stats['max_month']}", SECTION)
log_gold("INFO",
         f"distinct months: {stats['distinct_months']}, brands: {stats['distinct_brands']}, "
         f"cadenas: {stats['distinct_cadenas']}, canales: {stats['distinct_canales']}", SECTION)

coverage_dist = df_agg.groupBy("coverage_level").count().collect()
for row in coverage_dist:
    log_gold("INFO", f"coverage_level = {row['coverage_level']}: {row['count']:,} combinations", SECTION)

# COMMAND ----------
# MAGIC %md ## Step 6 — Save Output

# COMMAND ----------

save_gold_df(df_agg, "gold_sell_out_kpi", SECTION)

# COMMAND ----------
# MAGIC %md ## Step 7 — Register KPIs

# COMMAND ----------

_METRICS = [
    ("so_revenue_mxn",      "SELL_OUT", "SUM(REVENUE_SELL_OUT)",              "MXN",      "fecha_month x marca_std x canal_std x cadena_std"),
    ("so_vol_units",        "SELL_OUT", "SUM(VOL_SELL_OUT)",                  "Units",    "fecha_month x marca_std x canal_std x cadena_std"),
    ("so_pcs",              "SELL_OUT", "SUM(PCS_SELL_OUT)",                  "Pieces",   "fecha_month x marca_std x canal_std x cadena_std"),
    ("so_avg_price_mxn",    "SELL_OUT", "so_revenue_mxn / so_vol_units",      "MXN/unit", "fecha_month x marca_std x canal_std x cadena_std"),
    ("so_inventory_units",  "SELL_OUT", "SUM(VOL_INV)",                       "Units",    "fecha_month x marca_std x canal_std x cadena_std"),
    ("so_inventory_days",   "SELL_OUT", "(so_inventory_units*30)/so_vol_units","Days",     "fecha_month x marca_std x canal_std x cadena_std"),
    ("so_store_count",      "SELL_OUT", "COUNT(DISTINCT STORE_ID)",           "#stores",  "fecha_month x marca_std x canal_std x cadena_std"),
    ("so_sku_count",        "SELL_OUT", "COUNT(DISTINCT sku_ean_cod)",        "#SKUs",    "fecha_month x marca_std x canal_std x cadena_std"),
    ("coverage_level",      "SELL_OUT", "CASE on so_store_count vs thresholds","LOW/MED/HIGH","fecha_month x marca_std x canal_std x cadena_std"),
]
for m in _METRICS:
    register_gold_metric(*m)

log_gold("INFO", f"Registered {len(_METRICS)} SELL_OUT KPI metrics", SECTION)
log_gold("INFO", "GOLD_SELL_OUT — COMPLETE ✅", SECTION)
write_audit_log()
