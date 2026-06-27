# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 4 Gold — Investment KPIs
# MAGIC **gold_investment.py**
# MAGIC
# MAGIC Inputs:
# MAGIC   logs/mkt_on_std.csv  (7,282 rows)
# MAGIC   logs/mkt_off_std.csv (100,000 rows)
# MAGIC
# MAGIC Output:
# MAGIC   gold_investment_kpi.csv — Grain: fecha_month x marca_std x canal_std x brand_owner_type
# MAGIC
# MAGIC KPIs:
# MAGIC   inv_mkt_on_mxn, inv_mkt_off_mxn, inv_total_mxn,
# MAGIC   inv_on_pct, inv_campaign_count, inv_platform_count, inv_media_type_count
# MAGIC
# MAGIC F6 resolution:
# MAGIC   brand_owner_type = DANONE | COMPETITOR | UNKNOWN
# MAGIC   gold_commercial_kpi uses Danone rows only.
# MAGIC   Competitor rows are retained here for benchmark analysis.
# MAGIC   W5 warning is raised logging excluded competitor row count.

# COMMAND ----------

# MAGIC %run ./gold_kpi_utils

# COMMAND ----------

import os
from pyspark.sql import functions as F

SECTION = "INVESTMENT"
log_gold("INFO", "=" * 72, SECTION)
log_gold("INFO", "GOLD_INVESTMENT — START", SECTION)

check_run_mode()

# COMMAND ----------
# MAGIC %md ## Step 1 — Load Silver MKT_ON and MKT_OFF

# COMMAND ----------

# MKT_ON columns: FECHA, MARCA, MARCA_STD, CAMPANA, MEDIO, SOPORTE_PLATAFORMA,
#   CATEGORIA, CADENA_RAW, IMPRESIONES, CLICS, INVERSION_REAL,
#   FACT_ROW_COUNT, SOURCE_SYSTEM, STD_CREATED_AT, cadena_std, canal_std
MKT_ON_PATH  = os.path.join(LOGS_DIR, "mkt_on_std.csv")
# MKT_OFF columns: FECHA, MARCA, MARCA_STD, CAMPANA, MEDIO, SOPORTE_PLATAFORMA,
#   CATEGORIA, CLASE, CADENA_STD, INVERSION_REAL, IMPACTOS_HT,
#   FACT_ROW_COUNT, SOURCE_SYSTEM, STD_CREATED_AT, canal_std
MKT_OFF_PATH = os.path.join(LOGS_DIR, "mkt_off_std.csv")

log_gold("INFO", f"Loading: {MKT_ON_PATH}", SECTION)
df_on_raw = read_silver_csv(MKT_ON_PATH)

log_gold("INFO", f"Loading: {MKT_OFF_PATH}", SECTION)
df_off_raw = read_silver_csv(MKT_OFF_PATH)


on_count  = df_on_raw.count()
off_count = df_off_raw.count()
total_silver = on_count + off_count

log_gold("INFO", f"mkt_on_std: {on_count:,} rows | mkt_off_std: {off_count:,} rows | total: {total_silver:,}", SECTION)
gold_blocker("B1", on_count == 0 and off_count == 0, "Both MKT_ON and MKT_OFF inputs are empty", SECTION)

# COMMAND ----------
# MAGIC %md ## Step 2 — Normalize and Tag Source

# COMMAND ----------

# Select common columns before union, tagging investment_source
COMMON_COLS = ["FECHA", "MARCA_STD", "CAMPANA", "MEDIO", "SOPORTE_PLATAFORMA",
               "INVERSION_REAL", "canal_std"]

df_on_tagged = (
    df_on_raw
    .select(*COMMON_COLS)
    .withColumn("investment_source", F.lit("MKT_ON"))
)

# MKT_OFF: SOPORTE_PLATAFORMA may be null but column exists
df_off_tagged = (
    df_off_raw
    .select(*[c for c in COMMON_COLS if c in df_off_raw.columns])
    .withColumn("investment_source", F.lit("MKT_OFF"))
)

# Align schemas (SOPORTE_PLATAFORMA may be missing in MKT_OFF)
for col_name in COMMON_COLS:
    if col_name not in df_off_tagged.columns:
        df_off_tagged = df_off_tagged.withColumn(col_name, F.lit(None).cast("string"))

df = df_on_tagged.union(df_off_tagged.select(*COMMON_COLS + ["investment_source"]))

log_gold("INFO", f"Union of MKT_ON + MKT_OFF: {df.count():,} rows total", SECTION)

# COMMAND ----------
# MAGIC %md ## Step 3 — Month-Truncate (B13)

# COMMAND ----------

df = df.withColumn("fecha_month", month_trunc(F.to_date(F.col("FECHA"), "yyyy-MM-dd")))

null_fecha = df.filter(F.col("fecha_month").isNull()).count()
gold_blocker("B2", null_fecha > 0,
             f"Investment union has {null_fecha} NULL fecha_month rows", SECTION)
if null_fecha == 0:
    gold_passed("B2", "fecha_month has zero NULLs in investment data", SECTION)

# B3 — NULL MARCA_STD: some MKT_ON/MKT_OFF rows may have blank brand field in source.
# Quarantine NULLs rather than blocking — investment rows without brand cannot be attributed.
null_marca = df.filter(F.col("MARCA_STD").isNull()).count()
if null_marca > 0:
    gold_warn("W_MARCA", True,
              f"B3 QUARANTINE: {null_marca} investment rows have NULL MARCA_STD. "
              f"Excluding from Gold aggregation (cannot attribute brand-less investment).",
              SECTION)
    df = df.filter(F.col("MARCA_STD").isNotNull())
    valid_count = df.count()
    log_gold("INFO", f"After NULL MARCA_STD quarantine: {valid_count:,} valid rows remain", SECTION)
    gold_passed("B3", f"B3 cleared after quarantine: {valid_count:,} valid rows", SECTION)
else:
    gold_passed("B3", "MARCA_STD has zero NULLs in investment data", SECTION)


check_fecha_month_range(df, "investment_union")

# COMMAND ----------
# MAGIC %md ## Step 4 — Brand Owner Classification (F6)

# COMMAND ----------

# Classify brand_owner_type using Danone brand set from brand_crosswalk.yaml
# _DANONE_BRANDS_SET is populated by gold_kpi_utils._load_danone_brands()
danone_brands_list = list(_DANONE_BRANDS_SET)

if danone_brands_list:
    df = df.withColumn(
        "brand_owner_type",
        F.when(
            F.upper(F.trim(F.col("MARCA_STD"))).isin(danone_brands_list),
            F.lit("DANONE")
        ).otherwise(F.lit("COMPETITOR"))
    )
else:
    # Fallback: all brands marked UNKNOWN if crosswalk failed to load
    df = df.withColumn("brand_owner_type", F.lit("UNKNOWN"))
    gold_warn("W5", True,
              "brand_crosswalk.yaml not loaded — all brands classified as UNKNOWN. "
              "Master join will exclude all rows. Reload gold_kpi_utils.", SECTION)

brand_dist = df.groupBy("brand_owner_type").count().collect()
for row in brand_dist:
    log_gold("INFO", f"brand_owner_type = {row['brand_owner_type']}: {row['count']:,} rows", SECTION)

competitor_count = df.filter(F.col("brand_owner_type") != "DANONE").count()
gold_warn("W5", competitor_count > 0,
          f"W5: {competitor_count:,} competitor investment rows retained in gold_investment_kpi "
          f"but excluded from gold_commercial_kpi (Danone-only master).", SECTION)

# COMMAND ----------
# MAGIC %md ## Step 5 — Aggregate KPIs (Grain: fecha_month x MARCA_STD x canal_std x brand_owner_type)

# COMMAND ----------

GRAIN_KEYS = ["fecha_month", "MARCA_STD", "canal_std", "brand_owner_type"]

df_agg = (
    df.groupBy(*GRAIN_KEYS)
      .agg(
          F.sum(F.when(F.col("investment_source") == "MKT_ON",
                       F.col("INVERSION_REAL")).otherwise(0)).alias("inv_mkt_on_mxn"),
          F.sum(F.when(F.col("investment_source") == "MKT_OFF",
                       F.col("INVERSION_REAL")).otherwise(0)).alias("inv_mkt_off_mxn"),
          F.sum("INVERSION_REAL").alias("inv_total_mxn"),
          F.countDistinct("CAMPANA").alias("inv_campaign_count"),
          F.countDistinct("SOPORTE_PLATAFORMA").alias("inv_platform_count"),
          F.countDistinct("MEDIO").alias("inv_media_type_count"),
      )
      .withColumn(
          "inv_on_pct",
          safe_divide(F.col("inv_mkt_on_mxn"), F.col("inv_total_mxn"))
      )
      .withColumnRenamed("MARCA_STD", "marca_std")
)

agg_count = df_agg.count()
log_gold("INFO", f"gold_investment_kpi: {agg_count:,} rows at grain {GRAIN_KEYS}", SECTION)

# B8 — no fan-out
gold_blocker("B8", agg_count > total_silver,
             f"Investment output ({agg_count}) > Silver input ({total_silver}) — fan-out!",
             SECTION)

# B4 — no Inf/NaN
check_no_inf_nan(df_agg, ["inv_on_pct"], "gold_investment_kpi")

# W3 — negative investment
neg_inv = df_agg.filter(F.col("inv_total_mxn") < 0).count()
gold_warn("W3", neg_inv > 0,
          f"gold_investment_kpi has {neg_inv} rows with negative inv_total_mxn", SECTION)

# COMMAND ----------
# MAGIC %md ## Step 6 — Save Output

# COMMAND ----------

save_gold_df(df_agg, "gold_investment_kpi", SECTION)

# COMMAND ----------
# MAGIC %md ## Step 7 — Register KPIs

# COMMAND ----------

_METRICS = [
    ("inv_mkt_on_mxn",       "MKT_ON",      "SUM(INVERSION_REAL) WHERE source=MKT_ON",   "MXN",  "fecha_month x marca_std x canal_std x brand_owner_type"),
    ("inv_mkt_off_mxn",      "MKT_OFF",     "SUM(INVERSION_REAL) WHERE source=MKT_OFF",  "MXN",  "fecha_month x marca_std x canal_std x brand_owner_type"),
    ("inv_total_mxn",        "MKT_ON+OFF",  "SUM(INVERSION_REAL)",                       "MXN",  "fecha_month x marca_std x canal_std x brand_owner_type"),
    ("inv_on_pct",           "MKT_ON+OFF",  "inv_mkt_on_mxn / inv_total_mxn",            "%",    "fecha_month x marca_std x canal_std x brand_owner_type"),
    ("inv_campaign_count",   "MKT_ON+OFF",  "COUNT(DISTINCT CAMPANA)",                   "#",    "fecha_month x marca_std x canal_std x brand_owner_type"),
    ("inv_platform_count",   "MKT_ON",      "COUNT(DISTINCT SOPORTE_PLATAFORMA)",         "#",    "fecha_month x marca_std x canal_std x brand_owner_type"),
    ("inv_media_type_count", "MKT_ON+OFF",  "COUNT(DISTINCT MEDIO)",                     "#",    "fecha_month x marca_std x canal_std x brand_owner_type"),
]
for m in _METRICS:
    register_gold_metric(*m)

log_gold("INFO", f"Registered {len(_METRICS)} Investment KPI metrics", SECTION)
log_gold("INFO", "GOLD_INVESTMENT — COMPLETE ✅", SECTION)
write_audit_log()
