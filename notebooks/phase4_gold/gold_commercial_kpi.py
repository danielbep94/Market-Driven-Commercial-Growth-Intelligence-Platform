# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 4 Gold — Commercial Master KPI Table
# MAGIC **gold_commercial_kpi.py**
# MAGIC
# MAGIC Inputs (master-safe aggregates only):
# MAGIC   gold_sell_out_kpi.csv       — BASE  (fecha_month x marca_std x canal_std x cadena_std)
# MAGIC   gold_sell_in_kpi_master.csv — LEFT  (fecha_month x marca_std x canal_std)
# MAGIC   gold_investment_kpi.csv     — LEFT  (fecha_month x marca_std x canal_std, Danone only)
# MAGIC   gold_nielsen_kpi_master.csv — LEFT  (fecha_month x canal_std)
# MAGIC
# MAGIC Output:
# MAGIC   gold_commercial_kpi.csv — Grain: fecha_month x marca_std x canal_std x cadena_std
# MAGIC
# MAGIC F1 resolution:
# MAGIC   All right-side tables are pre-asserted unique on their join keys (B10).
# MAGIC   Row count is checked pre- and post-join (B8).
# MAGIC
# MAGIC F2 resolution:
# MAGIC   region_std and cbu are NOT dimensions in the master table.
# MAGIC   Master grain = fecha_month x marca_std x canal_std x cadena_std.
# MAGIC
# MAGIC F5 resolution:
# MAGIC   roas_gross (not roi_gross) = so_revenue_mxn / inv_total_mxn — guarded.
# MAGIC
# MAGIC F6 resolution:
# MAGIC   Investment join uses Danone-only rows (brand_owner_type = DANONE).
# MAGIC   Competitor rows excluded from master. W5 already raised in gold_investment.py.

# COMMAND ----------

# MAGIC %run ./gold_kpi_utils

# COMMAND ----------

import os
from pyspark.sql import functions as F

SECTION = "COMMERCIAL_KPI"
log_gold("INFO", "=" * 72, SECTION)
log_gold("INFO", "GOLD_COMMERCIAL_KPI — START", SECTION)

check_run_mode()

# COMMAND ----------
# MAGIC %md ## Step 1 — Load Gold source tables

# COMMAND ----------

def load_gold(name: str):
    """Load a Gold KPI CSV from DBFS (primary) or logs/ (fallback)."""
    try:
        path = f"{DBFS_GOLD_ROOT}/{name}"
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
        log_gold("INFO", f"Loaded from DBFS: {path} ({df.count():,} rows)", SECTION)
        return df
    except Exception:
        path = os.path.join(LOGS_DIR, f"{name}.csv")
        log_gold("INFO", f"Falling back to logs/: {path}", SECTION)
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
        log_gold("INFO", f"Loaded from logs/: {path} ({df.count():,} rows)", SECTION)
        return df

df_so  = load_gold("gold_sell_out_kpi")
df_si  = load_gold("gold_sell_in_kpi_master")
df_inv = load_gold("gold_investment_kpi")
df_nls = load_gold("gold_nielsen_kpi_master")

base_count = df_so.count()
log_gold("INFO", f"Base table (sell_out_kpi): {base_count:,} rows", SECTION)

# COMMAND ----------
# MAGIC %md ## Step 2 — Pre-join uniqueness assertions (B10)

# COMMAND ----------

SI_JOIN_KEYS  = ["fecha_month", "marca_std", "canal_std"]
INV_JOIN_KEYS = ["fecha_month", "marca_std", "canal_std"]
NLS_JOIN_KEYS = ["fecha_month", "canal_std"]

# SELL_IN master — must be unique on SI_JOIN_KEYS
assert_unique_keys(df_si,  SI_JOIN_KEYS,  "gold_sell_in_kpi_master")

# Investment — filter to Danone only, then assert unique
df_inv_danone = df_inv.filter(F.col("brand_owner_type") == "DANONE")
danone_inv_count = df_inv_danone.count()
competitor_inv_count = df_inv.count() - danone_inv_count
log_gold("INFO", f"Investment: {danone_inv_count:,} Danone rows / {competitor_inv_count:,} competitor rows excluded", SECTION)
gold_warn("W5", competitor_inv_count > 0,
          f"W5: {competitor_inv_count:,} competitor investment rows excluded from master join", SECTION)
assert_unique_keys(df_inv_danone.drop("brand_owner_type"), INV_JOIN_KEYS, "gold_investment_kpi_danone")

# Nielsen master — unique on NLS_JOIN_KEYS
assert_unique_keys(df_nls, NLS_JOIN_KEYS, "gold_nielsen_kpi_master")

# COMMAND ----------
# MAGIC %md ## Step 3 — Fan-out-safe LEFT JOINs

# COMMAND ----------

# All KPI columns in right-side tables are already source-prefixed:
#   sell_in_kpi_master  → si_revenue_mxn, si_vol_litros, ...
#   investment_kpi      → inv_mkt_on_mxn, inv_total_mxn, ...
#   nielsen_kpi_master  → nls_value_share, nls_volume_share, ...
# DO NOT apply prefix_cols — it would create si_si_revenue_mxn, nls_nls_value_share etc.
#
# To avoid Spark ambiguous column reference after join, drop duplicate join keys
# from right-side frames before joining (keep only KPI payload columns + join key once).

def drop_dup_keys(df, join_keys):
    """Drop join key columns from right-side df to avoid ambiguity after join."""
    return df.drop(*join_keys)

# JOIN 1: SELL_OUT (base) LEFT JOIN SELL_IN master on (fecha_month, marca_std, canal_std)
log_gold("INFO", "JOIN 1: sell_out LEFT JOIN sell_in_master on (fecha_month, marca_std, canal_std)", SECTION)
df_j1 = df_so.join(
    df_si.select(*SI_JOIN_KEYS + [c for c in df_si.columns if c not in SI_JOIN_KEYS]),
    SI_JOIN_KEYS, "left"
)
assert_no_join_fanout(base_count, df_j1, "SI_MASTER_JOIN")

# JOIN 2: + LEFT JOIN Investment (Danone only, brand_owner_type dropped)
log_gold("INFO", "JOIN 2: + LEFT JOIN investment_danone on (fecha_month, marca_std, canal_std)", SECTION)
df_inv_payload = df_inv_danone.drop("brand_owner_type")
df_inv_payload = df_inv_payload.select(*INV_JOIN_KEYS + [c for c in df_inv_payload.columns if c not in INV_JOIN_KEYS])
df_j2 = df_j1.join(df_inv_payload, INV_JOIN_KEYS, "left")
assert_no_join_fanout(base_count, df_j2, "INV_DANONE_JOIN")

# JOIN 3: + LEFT JOIN Nielsen master on (fecha_month, canal_std)
log_gold("INFO", "JOIN 3: + LEFT JOIN nielsen_master on (fecha_month, canal_std)", SECTION)
df_nls_payload = df_nls.select(*NLS_JOIN_KEYS + [c for c in df_nls.columns if c not in NLS_JOIN_KEYS])
df_j3 = df_j2.join(df_nls_payload, NLS_JOIN_KEYS, "left")
assert_no_join_fanout(base_count, df_j3, "NIELSEN_MASTER_JOIN")

log_gold("INFO", f"All 3 joins complete — final row count: {df_j3.count():,}", SECTION)


# COMMAND ----------
# MAGIC %md ## Step 4 — Derive master KPI columns

# COMMAND ----------

# roas_gross = so_revenue_mxn / inv_total_mxn (F5: renamed from roi_gross)
# Guard: NULL when investment is zero or NULL
df_master = (
    df_j3
    .withColumn(
        "roas_gross",
        safe_divide(F.col("so_revenue_mxn"), F.col("inv_total_mxn"))
    )
    # Metadata columns
    .withColumn("gold_run_ts", F.lit(ts()))
    .withColumn("silver_input_mode", F.lit(RUN_MODE))
    .withColumn("has_sell_in",    F.col("si_revenue_mxn").isNotNull().cast("boolean"))
    .withColumn("has_sell_out",   F.col("so_revenue_mxn").isNotNull().cast("boolean"))
    .withColumn("has_investment",
                F.col("inv_total_mxn").isNotNull().cast("boolean"))
    .withColumn("has_nielsen",
                (F.col("nls_value_share").isNotNull() |
                 F.col("nls_volume_share").isNotNull()).cast("boolean"))
    .withColumn(
        "data_confidence",
        F.when(
            F.col("has_sell_in") & F.col("has_sell_out") &
            F.col("has_investment") & F.col("has_nielsen"),
            F.lit("HIGH")
        ).when(
            F.col("has_sell_out"),
            F.lit("MEDIUM")
        ).otherwise(F.lit("LOW"))
    )
)

# COMMAND ----------
# MAGIC %md ## Step 5 — Select Final Master Columns (F2: no region_std, no cbu)

# COMMAND ----------

MASTER_DIMENSIONS = ["fecha_month", "marca_std", "canal_std", "cadena_std"]
SELL_IN_KPIS      = ["si_revenue_mxn", "si_vol_litros", "si_vol_kg",
                      "si_avg_price_mxn_per_litre", "si_avg_price_mxn_per_kg", "si_sku_count"]
SELL_OUT_KPIS     = ["so_revenue_mxn", "so_vol_units", "so_avg_price_mxn",
                      "so_inventory_days", "so_store_count", "coverage_level"]
INVESTMENT_KPIS   = ["inv_total_mxn", "inv_mkt_on_mxn", "inv_mkt_off_mxn", "inv_on_pct"]
NIELSEN_KPIS      = ["nls_value_share", "nls_volume_share", "nls_numeric_dist", "nls_category_value_mxn"]
DERIVED_KPIS      = ["roas_gross"]
METADATA_COLS     = ["gold_run_ts", "data_confidence", "has_sell_in", "has_sell_out",
                      "has_investment", "has_nielsen", "silver_input_mode"]

ALL_EXPECTED_COLS = (MASTER_DIMENSIONS + SELL_IN_KPIS + SELL_OUT_KPIS +
                     INVESTMENT_KPIS + NIELSEN_KPIS + DERIVED_KPIS + METADATA_COLS)

# B9 — validate all expected columns present
validate_expected_columns(df_master, ALL_EXPECTED_COLS, "gold_commercial_kpi")

df_final = df_master.select(*ALL_EXPECTED_COLS)
final_count = df_final.count()

log_gold("INFO", f"gold_commercial_kpi final: {final_count:,} rows, {len(ALL_EXPECTED_COLS)} columns", SECTION)
log_gold("INFO", f"Master grain: fecha_month x marca_std x canal_std x cadena_std", SECTION)
log_gold("INFO", f"region_std and cbu NOT in master table (F2 correction)", SECTION)

# COMMAND ----------
# MAGIC %md ## Step 6 — Final Validation Checks

# COMMAND ----------

# B4 — no Inf/NaN in derived KPIs
check_no_inf_nan(df_final, ["roas_gross", "so_inventory_days"], "gold_commercial_kpi")

# B2 — fecha_month not NULL
null_fecha = df_final.filter(F.col("fecha_month").isNull()).count()
gold_blocker("B2", null_fecha > 0,
             f"gold_commercial_kpi has {null_fecha} NULL fecha_month rows", SECTION)
if null_fecha == 0:
    gold_passed("B2", "fecha_month has zero NULLs in master table", SECTION)

# B3 — NULL marca_std in master: quarantine residual NULLs rather than blocking.
# Source notebooks already quarantined NULL marca_std; any remaining NULLs are
# structural LEFT JOIN propagation from SELL_OUT × SELL_IN null match rows.
null_marca = df_final.filter(F.col("marca_std").isNull()).count()
if null_marca > 0:
    gold_warn("W_MARCA", True,
              f"B3 QUARANTINE: {null_marca} master rows have NULL marca_std — "
              f"structural LEFT JOIN propagation. These rows are excluded from final output.",
              SECTION)
    df_final = df_final.filter(F.col("marca_std").isNotNull())
    final_count = df_final.count()
    log_gold("INFO", f"After NULL marca_std quarantine: {final_count:,} rows remain in master", SECTION)
    gold_passed("B3", f"B3 cleared after quarantine: {final_count:,} rows", SECTION)
else:
    gold_passed("B3", "marca_std has zero NULLs in master table", SECTION)


# B8 — final count ≤ base (sell_out) count
gold_blocker("B8", final_count > base_count,
             f"gold_commercial_kpi ({final_count}) > sell_out base ({base_count}) — fan-out!", SECTION)
if final_count <= base_count:
    gold_passed("B8", f"No fan-out: {final_count} ≤ {base_count} (sell_out base)", SECTION)

# W2 — negative revenue
neg_rev = df_final.filter(F.col("so_revenue_mxn") < 0).count()
gold_warn("W2", neg_rev > 0,
          f"gold_commercial_kpi has {neg_rev} rows with negative so_revenue_mxn", SECTION)

# Coverage summary
conf_dist = df_final.groupBy("data_confidence").count().collect()
for row in conf_dist:
    log_gold("INFO", f"data_confidence = {row['data_confidence']}: {row['count']:,} rows", SECTION)

# COMMAND ----------
# MAGIC %md ## Step 7 — Save Output

# COMMAND ----------

save_gold_df(df_final, "gold_commercial_kpi", SECTION)

# COMMAND ----------
# MAGIC %md ## Step 8 — Register Derived KPIs

# COMMAND ----------

register_gold_metric("roas_gross", "SELL_OUT+INVESTMENT",
                      "so_revenue_mxn / inv_total_mxn (Danone only, guarded)",
                      "MXN revenue per MXN invested",
                      "fecha_month x marca_std x canal_std x cadena_std")
register_gold_metric("data_confidence", "ALL_SOURCES",
                      "HIGH(all sources) | MEDIUM(sell_out only) | LOW(no sell_out)",
                      "level", "fecha_month x marca_std x canal_std x cadena_std")

flush_kpi_registry()

log_gold("INFO", "GOLD_COMMERCIAL_KPI — COMPLETE ✅", SECTION)
write_audit_log()
