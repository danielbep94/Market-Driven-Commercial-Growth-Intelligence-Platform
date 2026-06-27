# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 4 Gold — SELL_IN KPIs
# MAGIC **gold_sell_in.py**
# MAGIC
# MAGIC Input:  `logs/sell_in_std.csv` (49,815 rows)
# MAGIC
# MAGIC Outputs:
# MAGIC - `gold_sell_in_kpi.csv`        Grain: fecha_month x marca_std x canal_std x CBU
# MAGIC - `gold_sell_in_kpi_master.csv` Grain: fecha_month x marca_std x canal_std (master-safe, no CBU)
# MAGIC
# MAGIC KPIs:
# MAGIC   si_revenue_mxn, si_vol_litros, si_vol_kg,
# MAGIC   si_avg_price_mxn_per_litre, si_avg_price_mxn_per_kg,
# MAGIC   si_sku_count, si_transaction_count
# MAGIC
# MAGIC Architectural rules:
# MAGIC   - B12: RUN_MODE must be FULL
# MAGIC   - B13: fecha_month month-truncated
# MAGIC   - B11: fecha_month within GOLD_START_MONTH / GOLD_END_MONTH
# MAGIC   - B4:  no Inf/NaN in derived KPIs
# MAGIC   - B10: master output asserted unique on (fecha_month, marca_std, canal_std)

# COMMAND ----------

# MAGIC %run ./gold_kpi_utils

# COMMAND ----------

import os
import pandas as pd
from pyspark.sql import functions as F

SECTION = "SELL_IN"
log_gold("INFO", "=" * 72, SECTION)
log_gold("INFO", "GOLD_SELL_IN — START", SECTION)

# B12: block SAMPLE mode
check_run_mode()

# COMMAND ----------

# MAGIC %md ## Step 1 — Load Silver sell_in_std.csv

# COMMAND ----------

# DBTITLE 1,Cell 5
# Actual column names from Silver header:
# MAT_IDT, SKU_EAN_COD, MAT_LCL_DSC, MARCA_STD, CBU, CANAL_RAW, CUS_GRP_DSC,
# DIS_CHL_COD, YEAR_MONTH, REVENUE_MXN, VOLUME_KGR, VOLUME_LITER, CASES, SKU_QTY,
# FACT_ROW_COUNT, SOURCE_SYSTEM, STD_CREATED_AT, cadena_std, canal_std

SELL_IN_PATH = os.path.join(LOGS_DIR, "sell_in_std.csv")
log_gold("INFO", f"Loading Silver input: {SELL_IN_PATH}", SECTION)

# read_silver_csv() handles Workspace paths via pandas bridge (Spark cannot
# read /Workspace/ paths on distributed executors — driver-only access).
df_raw = read_silver_csv(SELL_IN_PATH)

silver_count = df_raw.count()
log_gold("INFO", f"Silver sell_in_std loaded: {silver_count:,} rows", SECTION)

# Hard blocker B1 — non-empty input
gold_blocker("B1", silver_count == 0, "sell_in_std.csv is empty", SECTION)

# COMMAND ----------

# MAGIC %md ## Step 2 — Month-Truncate (B13)

# COMMAND ----------

# YEAR_MONTH is yyyyMM integer format (e.g. 202501) → parse to date → truncate to 1st of month
df = (
    df_raw
    .withColumn(
        "fecha_month",
        month_trunc(
            F.to_date(F.col("YEAR_MONTH").cast("string"), "yyyyMM")
        )
    )
)

log_gold("INFO", "fecha_month column created (month-truncated from YEAR_MONTH)", SECTION)

# B3 — NULL MARCA_STD: Phase 3 Silver LEFT JOIN to V_D_ITEM produces structural NULLs
# for fact rows whose MAT_IDT has no product match. Quarantine them; do not block.
null_marca = df.filter(F.col("MARCA_STD").isNull()).count()
if null_marca > 0:
    gold_warn("W_MARCA", True,
              f"B3 QUARANTINE: {null_marca} rows have NULL MARCA_STD (unmatched product LEFT JOIN). "
              f"Excluding from Gold aggregation. Matches Phase 3 architectural decision R9.",
              SECTION)
    df = df.filter(F.col("MARCA_STD").isNotNull())
    valid_count = df.count()
    log_gold("INFO", f"After NULL MARCA_STD quarantine: {valid_count:,} valid rows remain", SECTION)
    gold_passed("B3", f"B3 cleared after quarantine: {valid_count:,} valid rows", SECTION)
else:
    gold_passed("B3", "MARCA_STD has zero NULLs", SECTION)


# B2 — fecha_month must not be NULL
null_fecha = df.filter(F.col("fecha_month").isNull()).count()
gold_blocker("B2", null_fecha > 0,
             f"sell_in_std has {null_fecha} NULL fecha_month rows", SECTION)
if null_fecha == 0:
    gold_passed("B2", "fecha_month has zero NULLs", SECTION)

# B11 + B13 — range and truncation check
check_fecha_month_range(df, "sell_in_std")

# COMMAND ----------

# MAGIC %md ## Step 3 — Detailed KPI Aggregation (Grain: fecha_month x marca_std x canal_std x CBU)

# COMMAND ----------

DETAIL_KEYS = ["fecha_month", "MARCA_STD", "canal_std", "CBU"]

df_detail = (
    df.groupBy(*DETAIL_KEYS)
      .agg(
          F.sum("REVENUE_MXN").alias("si_revenue_mxn"),
          F.sum("VOLUME_LITER").alias("si_vol_litros"),
          F.sum("VOLUME_KGR").alias("si_vol_kg"),
          F.countDistinct("SKU_EAN_COD").alias("si_sku_count"),
          F.count("*").alias("si_transaction_count"),
      )
      # Derived: average net price — guarded division, separate by unit type
      .withColumn(
          "si_avg_price_mxn_per_litre",
          safe_divide(F.col("si_revenue_mxn"), F.col("si_vol_litros"))
      )
      .withColumn(
          "si_avg_price_mxn_per_kg",
          safe_divide(F.col("si_revenue_mxn"), F.col("si_vol_kg"))
      )
      # Rename to canonical lowercase dimension names
      .withColumnRenamed("MARCA_STD", "marca_std")
      .withColumnRenamed("CBU", "cbu")
)

detail_count = df_detail.count()
log_gold("INFO", f"Detailed gold_sell_in_kpi: {detail_count:,} rows at grain {DETAIL_KEYS}", SECTION)

# B8 — no fan-out vs Silver input
gold_blocker("B8", detail_count > silver_count,
             f"Detailed sell_in output ({detail_count}) > Silver input ({silver_count}) — fan-out!",
             SECTION)

# B4 — no Inf/NaN in derived KPIs
check_no_inf_nan(df_detail, ["si_avg_price_mxn_per_litre", "si_avg_price_mxn_per_kg"], "gold_sell_in_kpi")

# COMMAND ----------

# MAGIC %md ## Step 4 — Master-Safe Aggregation (Grain: fecha_month x marca_std x canal_std)

# COMMAND ----------

MASTER_KEYS = ["fecha_month", "marca_std", "canal_std"]

df_master = (
    df_detail.groupBy(*MASTER_KEYS)
             .agg(
                 F.sum("si_revenue_mxn").alias("si_revenue_mxn"),
                 F.sum("si_vol_litros").alias("si_vol_litros"),
                 F.sum("si_vol_kg").alias("si_vol_kg"),
                 F.sum("si_sku_count").alias("si_sku_count"),
                 F.sum("si_transaction_count").alias("si_transaction_count"),
             )
             .withColumn(
                 "si_avg_price_mxn_per_litre",
                 safe_divide(F.col("si_revenue_mxn"), F.col("si_vol_litros"))
             )
             .withColumn(
                 "si_avg_price_mxn_per_kg",
                 safe_divide(F.col("si_revenue_mxn"), F.col("si_vol_kg"))
             )
)

master_count = df_master.count()
log_gold("INFO", f"Master gold_sell_in_kpi_master: {master_count:,} rows at grain {MASTER_KEYS}", SECTION)

# B10 — assert uniqueness on master join keys
assert_unique_keys(df_master, MASTER_KEYS, "gold_sell_in_kpi_master")

# B4 — derived KPI check
check_no_inf_nan(df_master, ["si_avg_price_mxn_per_litre", "si_avg_price_mxn_per_kg"],
                 "gold_sell_in_kpi_master")

# COMMAND ----------

# MAGIC %md ## Step 5 — Coverage log

# COMMAND ----------

# Log min/max month (W1 — missing source coverage)
month_stats = df_master.agg(
    F.min("fecha_month").alias("min_month"),
    F.max("fecha_month").alias("max_month"),
    F.countDistinct("fecha_month").alias("distinct_months"),
    F.countDistinct("marca_std").alias("distinct_brands"),
).collect()[0]

log_gold("INFO", f"fecha_month range: {month_stats['min_month']} → {month_stats['max_month']}", SECTION)
log_gold("INFO", f"distinct months: {month_stats['distinct_months']}, distinct brands: {month_stats['distinct_brands']}", SECTION)

# COMMAND ----------

# MAGIC %md ## Step 6 — Save Outputs

# COMMAND ----------

save_gold_df(df_detail, "gold_sell_in_kpi", SECTION)
save_gold_df(df_master, "gold_sell_in_kpi_master", SECTION)

# COMMAND ----------

# MAGIC %md ## Step 7 — Register KPIs

# COMMAND ----------

_METRICS = [
    ("si_revenue_mxn",              "SELL_IN", "SUM(REVENUE_MXN)",                      "MXN",    "fecha_month x marca_std x canal_std"),
    ("si_vol_litros",               "SELL_IN", "SUM(VOLUME_LITER)",                      "Litres", "fecha_month x marca_std x canal_std"),
    ("si_vol_kg",                   "SELL_IN", "SUM(VOLUME_KGR)",                        "KG",     "fecha_month x marca_std x canal_std"),
    ("si_avg_price_mxn_per_litre",  "SELL_IN", "si_revenue_mxn / si_vol_litros",         "MXN/L",  "fecha_month x marca_std x canal_std"),
    ("si_avg_price_mxn_per_kg",     "SELL_IN", "si_revenue_mxn / si_vol_kg",             "MXN/KG", "fecha_month x marca_std x canal_std"),
    ("si_sku_count",                "SELL_IN", "COUNT(DISTINCT SKU_EAN_COD)",             "#SKUs",  "fecha_month x marca_std x canal_std"),
    ("si_transaction_count",        "SELL_IN", "COUNT(*)",                               "#rows",  "fecha_month x marca_std x canal_std"),
]
for m in _METRICS:
    register_gold_metric(*m)

log_gold("INFO", f"Registered {len(_METRICS)} SELL_IN KPI metrics", SECTION)
log_gold("INFO", "GOLD_SELL_IN — COMPLETE ✅", SECTION)
write_audit_log()

# COMMAND ----------


