# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 5 Analytics Mart — Mart Builder
# MAGIC **mart_builder.py**
# MAGIC
# MAGIC Input:  `dbfs:/mnt/mdp/mdm/phase4_gold/data/gold_commercial_kpi` (4,427 rows)
# MAGIC
# MAGIC Output:
# MAGIC - `dbfs:/mnt/mdp/mdm/phase5_mart/mart_commercial_kpi_monthly/`  (Parquet)
# MAGIC - `logs/mart_commercial_kpi_monthly_pbi.csv`                    (Power BI flat CSV)
# MAGIC
# MAGIC Grain: `fecha_month × marca_std × canal_std × cadena_std`
# MAGIC
# MAGIC Zero Snowflake writes. DBFS + logs/ only. B14/B15 pre-confirmed.

# COMMAND ----------

# MAGIC %run ./mart_utils

# COMMAND ----------

from pyspark.sql import functions as F, Window
from pyspark.sql.types import DoubleType, LongType, IntegerType, StringType, DateType, TimestampType

SECTION = "MART_BUILDER"

log_mart("INFO", "=" * 72, SECTION)
log_mart("INFO", "GOLD_COMMERCIAL_KPI MART BUILDER — START", SECTION)
log_mart("INFO", f"RUN_MODE = {MART_RUN_MODE}", SECTION)

# ── Resolve pipeline run ID ───────────────────────────────────────────────────
PIPELINE_RUN_ID, RUN_ID_SOURCE = _resolve_pipeline_run_id()
log_mart("INFO", f"pipeline_run_id = {PIPELINE_RUN_ID} (source: {RUN_ID_SOURCE})", SECTION)

# COMMAND ----------
# MAGIC %md ## Step 1 — Load Gold

# COMMAND ----------

df = read_gold_df(GOLD_TABLE_NAME)
gold_row_count = df.count()

log_mart("INFO", f"Gold rows loaded: {gold_row_count:,}", SECTION)

if mart_blocker("MG1B_PRECHECK", gold_row_count == 0,
                "Gold table is empty — cannot build mart", SECTION):
    raise ValueError("Gold table empty")

if gold_row_count != GOLD_EXPECTED_ROWS:
    log_mart("⚠️  WARNING",
             f"Gold row count {gold_row_count:,} differs from expected {GOLD_EXPECTED_ROWS:,}. "
             "Proceeding — reconciliation will document difference.", SECTION)
else:
    mart_passed("GOLD_COUNT", f"Gold row count confirmed: {gold_row_count:,}", SECTION)

# COMMAND ----------
# MAGIC %md ## Step 1b — Enforce Gold numeric types (CSV inferSchema guard)

# COMMAND ----------

# Gold was saved as CSV partitions in Phase 4.
# Spark CSV inferSchema can return StringType for numeric columns when the file
# contains nulls or is read without an explicit schema. Cast all numeric Gold
# columns explicitly to their correct types before any derivation.

GOLD_NUMERIC_CASTS = {
    # Sell-in
    "si_revenue_mxn":            DoubleType(),
    "si_vol_litros":             DoubleType(),
    "si_vol_kg":                 DoubleType(),
    "si_avg_price_mxn_per_litre": DoubleType(),
    "si_avg_price_mxn_per_kg":   DoubleType(),
    "si_sku_count":              DoubleType(),
    # Sell-out
    "so_revenue_mxn":            DoubleType(),
    "so_vol_units":              DoubleType(),
    "so_avg_price_mxn":          DoubleType(),
    "so_inventory_days":         DoubleType(),
    "so_store_count":            DoubleType(),
    # Investment
    "inv_total_mxn":             DoubleType(),
    "inv_mkt_on_mxn":            DoubleType(),
    "inv_mkt_off_mxn":           DoubleType(),
    "inv_on_pct":                DoubleType(),
    # Nielsen
    "nls_value_share":           DoubleType(),
    "nls_volume_share":          DoubleType(),
    "nls_numeric_dist":          DoubleType(),
    "nls_category_value_mxn":    DoubleType(),
    # Derived (already in Gold)
    "roas_gross":                DoubleType(),
}

for col_name, dtype in GOLD_NUMERIC_CASTS.items():
    if col_name in df.columns:
        df = df.withColumn(col_name, F.col(col_name).cast(dtype))

log_mart("INFO",
         f"Explicit numeric cast applied to {len(GOLD_NUMERIC_CASTS)} Gold columns "
         "(CSV inferSchema guard — prevents MG14 StringType failures)",
         SECTION)

# Spot-check types
bad_types = []
schema_map_gold = {f.name: type(f.dataType).__name__ for f in df.schema.fields}
for col_name in GOLD_NUMERIC_CASTS:
    if col_name in schema_map_gold and schema_map_gold[col_name] == "StringType":
        bad_types.append(col_name)
if bad_types:
    raise ValueError(f"Cast failed — still StringType after explicit cast: {bad_types}")
else:
    log_mart("INFO", "All Gold numeric columns confirmed as non-StringType after cast", SECTION)

# COMMAND ----------
# MAGIC %md ## Step 2 — Date dimensions

# COMMAND ----------

df = (df
      .withColumn("fecha_month",  F.col("fecha_month").cast(DateType()))
      .withColumn("month_key",    F.date_format(F.col("fecha_month"), "yyyyMM").cast(LongType()))
      .withColumn("year_number",  F.year(F.col("fecha_month")).cast(IntegerType()))
      .withColumn("month_number", F.month(F.col("fecha_month")).cast(IntegerType()))
)
log_mart("INFO", "Date dimensions derived: month_key, year_number, month_number", SECTION)


# COMMAND ----------
# MAGIC %md ## Step 3 — Window spec (finest mart grain)

# COMMAND ----------

# All MoM derivations use the same finest-grain window
w_grain = (Window
           .partitionBy("marca_std", "canal_std", "cadena_std")
           .orderBy("fecha_month"))

log_mart("INFO",
         "Window spec: partitionBy(marca_std, canal_std, cadena_std).orderBy(fecha_month)",
         SECTION)

# COMMAND ----------
# MAGIC %md ## Step 4 — PRE-PROJECTION: sell_out_sell_in_ratio (MG21 validated here)

# COMMAND ----------

# Compute ratio while si_revenue_mxn is still present in the working dataframe
df = df.withColumn(
    "sell_out_sell_in_ratio",
    safe_divide(F.col("so_revenue_mxn"), F.col("si_revenue_mxn"))
)

# MG21 pre-projection validation: ratio must be NULL when si_revenue_mxn is NULL or 0
invalid_ratio = df.filter(
    (F.col("si_revenue_mxn").isNull() | (F.col("si_revenue_mxn") == 0)) &
    F.col("sell_out_sell_in_ratio").isNotNull()
).count()

if not mart_blocker("MG21", invalid_ratio > 0,
                    f"sell_out_sell_in_ratio is non-NULL for {invalid_ratio:,} rows where "
                    "si_revenue_mxn is NULL or 0 — safe_divide guard failed", SECTION):
    mart_passed("MG21", "sell_out_sell_in_ratio is NULL when si_revenue_mxn is NULL/0", SECTION)

log_mart("INFO", "sell_out_sell_in_ratio computed (pre-projection, MG21 validated)", SECTION)

# COMMAND ----------
# MAGIC %md ## Step 5 — mom_revenue_growth_pct

# COMMAND ----------

prior_rev = F.lag("so_revenue_mxn").over(w_grain)
df = df.withColumn(
    "mom_revenue_growth_pct",
    safe_divide(F.col("so_revenue_mxn") - prior_rev, prior_rev)
)
# NULL when prior is NULL, missing, or 0 (first month per grain = NULL)
log_mart("INFO", "mom_revenue_growth_pct derived (MoM % Δ so_revenue_mxn, finest grain)", SECTION)

# COMMAND ----------
# MAGIC %md ## Step 6 — share_change_pp

# COMMAND ----------

prior_share = F.lag("nls_value_share").over(w_grain)
df = df.withColumn(
    "share_change_pp",
    F.when(
        F.col("nls_value_share").isNull() | prior_share.isNull(),
        F.lit(None)
    ).otherwise(F.col("nls_value_share") - prior_share)
)
log_mart("INFO", "share_change_pp derived (MoM Δ nls_value_share, pp)", SECTION)

# COMMAND ----------
# MAGIC %md ## Step 7 — market_category_growth_pct (PoP % change — NOT direct map)

# COMMAND ----------

prior_cat = F.lag("nls_category_value_mxn").over(w_grain)
df = df.withColumn(
    "market_category_growth_pct",
    safe_divide(F.col("nls_category_value_mxn") - prior_cat, prior_cat)
)
# NULL when prior is NULL or 0 — derived from Gold nls_category_value_mxn PoP growth
log_mart("INFO",
         "market_category_growth_pct derived (PoP % Δ nls_category_value_mxn — not direct map)",
         SECTION)

# COMMAND ----------
# MAGIC %md ## Step 8 — market_adjusted_growth

# COMMAND ----------

df = df.withColumn(
    "market_adjusted_growth",
    F.when(
        F.col("mom_revenue_growth_pct").isNull() | F.col("market_category_growth_pct").isNull(),
        F.lit(None)
    ).otherwise(
        F.col("mom_revenue_growth_pct") - F.col("market_category_growth_pct")
    )
)
log_mart("INFO", "market_adjusted_growth derived (mom_revenue_growth_pct − market_category_growth_pct)", SECTION)

# COMMAND ----------
# MAGIC %md ## Step 9 — sell_out_coverage_pct (P7/P8: MEDIUM + MED → 0.75)

# COMMAND ----------

df = df.withColumn(
    "sell_out_coverage_pct",
    F.when(F.upper(F.trim(F.col("coverage_level"))) == "HIGH",
           F.lit(1.0).cast(DoubleType()))
     .when(F.upper(F.trim(F.col("coverage_level"))).isin("MEDIUM", "MED"),
           F.lit(0.75).cast(DoubleType()))
     .when(F.upper(F.trim(F.col("coverage_level"))) == "LOW",
           F.lit(0.5).cast(DoubleType()))
     .otherwise(F.lit(None).cast(DoubleType()))
)
log_mart("INFO",
         "sell_out_coverage_pct mapped: HIGH=1.0, MEDIUM/MED=0.75, LOW=0.5, else NULL",
         SECTION)

# COMMAND ----------
# MAGIC %md ## Step 10 — Rename Gold columns to DDL names

# COMMAND ----------

df = (df
      .withColumnRenamed("so_revenue_mxn",       "sell_out_revenue")
      .withColumnRenamed("so_vol_units",          "sell_out_units")
      .withColumnRenamed("si_vol_litros",         "sell_in_units")       # PROXY: liters not discrete units
      .withColumnRenamed("inv_total_mxn",         "investment_spend")
      .withColumnRenamed("nls_value_share",       "value_share")
      .withColumnRenamed("nls_volume_share",      "volume_share")
      .withColumnRenamed("nls_numeric_dist",      "numeric_distribution")
      .withColumnRenamed("data_confidence",       "data_confidence_overall")
)
log_mart("INFO", "Gold columns renamed to DDL mart names", SECTION)
log_mart("⚠️  WARNING MG28",
         "sell_in_units mapped from si_vol_litros (proxy). "
         "Represents sell-in volume in liters, not discrete unit count. "
         "Power BI users should note the unit label.", SECTION)

# COMMAND ----------
# MAGIC %md ## Step 11 — NULL placeholders for Phase 6 ML + deferred cols

# COMMAND ----------

PHASE6_COLS = {
    "fiscal_period":          StringType(),
    "waste_units":            DoubleType(),
    "waste_cost":             DoubleType(),
    "waste_rate":             DoubleType(),
    "waste_adjusted_roi":     DoubleType(),
    "forecast_units":         DoubleType(),
    "forecast_bias":          DoubleType(),
    "forecast_accuracy_wape": DoubleType(),
    "weighted_distribution":  DoubleType(),
    "price_index_vs_category": DoubleType(),
    "waste_risk_score":       DoubleType(),
    "waste_risk_category":    StringType(),
    "competitive_risk_score": DoubleType(),
    "recommended_action":     StringType(),
    "recommendation_rationale": StringType(),
    "recommendation_version": StringType(),
    "nielsen_lag_weeks":      IntegerType(),
}
for col_name, dtype in PHASE6_COLS.items():
    df = df.withColumn(col_name, F.lit(None).cast(dtype))

# W6 deferred GQS columns
df = (df
      .withColumn("growth_quality_score",  F.lit(None).cast(DoubleType()))
      .withColumn("gqs_confidence_level",  F.lit(None).cast(StringType()))
      .withColumn("gqs_version",           F.lit(None).cast(StringType()))
)
log_mart("INFO",
         f"Added {len(PHASE6_COLS) + 3} NULL placeholder columns (Phase 6 ML + W6 GQS deferred)",
         SECTION)

# COMMAND ----------
# MAGIC %md ## Step 12 — Audit columns

# COMMAND ----------

df = (df
      .withColumn("mart_computed_at", F.current_timestamp())
      .withColumn("pipeline_run_id",  F.lit(PIPELINE_RUN_ID).cast(StringType()))
)
log_mart("INFO", f"Audit columns added — mart_computed_at, pipeline_run_id={PIPELINE_RUN_ID}", SECTION)

# COMMAND ----------
# MAGIC %md ## Step 13 — Final column selection (approved mart contract, DDL order)

# COMMAND ----------

# Approved Phase 5 mart contract — DDL order + approved extras (roas_gross, mom_revenue_growth_pct)
APPROVED_MART_COLS = [
    # Dimensions
    "fecha_month", "marca_std", "canal_std", "cadena_std",
    # Date dims
    "month_key", "year_number", "month_number", "fiscal_period",
    # Sell-in
    "sell_in_units",           # PROXY: si_vol_litros (liters) — MG28
    # Sell-out
    "sell_out_units", "waste_units",
    "sell_out_revenue", "waste_cost", "investment_spend",
    # Ratios
    "sell_out_sell_in_ratio", "waste_rate", "waste_adjusted_roi",
    # Forecast (Phase 6)
    "forecast_units", "forecast_bias", "forecast_accuracy_wape",
    # Nielsen
    "value_share", "volume_share", "share_change_pp",
    "market_category_growth_pct", "market_adjusted_growth",
    "numeric_distribution", "weighted_distribution", "price_index_vs_category",
    # GQS (W6 deferred)
    "growth_quality_score", "gqs_confidence_level", "gqs_version",
    # ML (Phase 6)
    "waste_risk_score", "waste_risk_category", "competitive_risk_score",
    "recommended_action", "recommendation_rationale", "recommendation_version",
    # DQ / confidence
    "sell_out_coverage_pct", "nielsen_lag_weeks", "data_confidence_overall",
    # Audit
    "mart_computed_at", "pipeline_run_id",
    # Approved Phase 5 extra columns (explicitly approved in plan v3)
    "roas_gross",
    "mom_revenue_growth_pct",
]

# Validate all approved cols exist before projection
missing_before_select = [c for c in APPROVED_MART_COLS if c not in df.columns]
if missing_before_select:
    raise ValueError(f"Missing columns before final select: {missing_before_select}")

df_mart = df.select(*APPROVED_MART_COLS)
mart_row_count = df_mart.count()
log_mart("INFO",
         f"Final mart: {mart_row_count:,} rows × {len(APPROVED_MART_COLS)} columns",
         SECTION)

# COMMAND ----------
# MAGIC %md ## Step 14 — Grain + Inf/NaN validation

# COMMAND ----------

assert_unique_keys(df_mart, MART_GRAIN, "mart_commercial_kpi_monthly")

check_no_inf_nan(df_mart,
                 ["roas_gross", "sell_out_sell_in_ratio",
                  "mom_revenue_growth_pct", "share_change_pp",
                  "market_category_growth_pct", "market_adjusted_growth",
                  "sell_out_coverage_pct"],
                 "mart_commercial_kpi_monthly", "MG6/MG7/MG20/MG22")

# COMMAND ----------
# MAGIC %md ## Step 15 — Save Parquet to DBFS

# COMMAND ----------

dbfs_row_count = save_mart_df(df_mart, "mart_commercial_kpi_monthly", SECTION)

# COMMAND ----------
# MAGIC %md ## Step 16 — Save Power BI CSV to logs/

# COMMAND ----------

csv_row_count = save_mart_csv(df_mart, SECTION)

if mart_blocker("MG25", csv_row_count != dbfs_row_count,
                f"CSV rows ({csv_row_count:,}) ≠ DBFS rows ({dbfs_row_count:,})", SECTION):
    pass
else:
    mart_passed("MG25", f"CSV row count = DBFS row count = {csv_row_count:,}", SECTION)

# COMMAND ----------
# MAGIC %md ## Step 17 — Register KPIs

# COMMAND ----------

_grain_str = "fecha_month × marca_std × canal_std × cadena_std"

_METRIC_DEFS = [
    ("fecha_month",               "GOLD",        "fecha_month",              "Month dimension (1st of month)", _grain_str, "NOT NULL",  "ACTIVE",   ""),
    ("marca_std",                 "GOLD",        "marca_std",                "Standardised brand name",        _grain_str, "NOT NULL",  "ACTIVE",   ""),
    ("canal_std",                 "GOLD",        "canal_std",                "Standardised trade channel",     _grain_str, "NOT NULL",  "ACTIVE",   ""),
    ("cadena_std",                "GOLD",        "cadena_std",               "Standardised retail chain",      _grain_str, "NULLABLE",  "ACTIVE",   "NULL by design for SELL_IN (R9)"),
    ("sell_in_units",             "SELL_IN",     "si_vol_litros",            "Sell-in volume",                 _grain_str, "NULLABLE",  "PROXY",    "Maps si_vol_litros. Represents sell-in volume in liters, not discrete unit count — MG28"),
    ("sell_out_units",            "SELL_OUT",    "so_vol_units",             "Sell-out volume in units",       _grain_str, "NULLABLE",  "ACTIVE",   ""),
    ("sell_out_revenue",          "SELL_OUT",    "so_revenue_mxn",           "Sell-out revenue MXN",          _grain_str, "NULLABLE",  "ACTIVE",   ""),
    ("investment_spend",          "INVESTMENT",  "inv_total_mxn",            "Total investment MXN",          _grain_str, "NULLABLE",  "ACTIVE",   "NULL when no Danone investment for grain"),
    ("sell_out_sell_in_ratio",    "DERIVED",     "so_revenue_mxn/si_revenue_mxn", "Revenue sell-through ratio", _grain_str, "NULL when si=0/NULL", "ACTIVE", "MXN/MXN. Pre-projection validation MG21"),
    ("roas_gross",                "GOLD",        "roas_gross",               "Gross ROAS (so_revenue/inv)",   _grain_str, "NULL when inv=0/NULL", "ACTIVE", "APPROVED Phase 5 extra column"),
    ("mom_revenue_growth_pct",    "DERIVED",     "MoM % Δ so_revenue_mxn",  "Month-over-month revenue growth %", _grain_str, "NULL — first month per grain", "ACTIVE", "APPROVED Phase 5 extra column. Finest grain window."),
    ("share_change_pp",           "DERIVED",     "Δ nls_value_share",        "MoM share change (pp)",         _grain_str, "NULL — first month or NULL share", "ACTIVE", ""),
    ("market_category_growth_pct","DERIVED",     "PoP % Δ nls_category_value_mxn", "Category value growth %", _grain_str, "NULL — first month or NULL value", "ACTIVE", "Derived from Gold nls_category_value_mxn PoP growth — NOT direct map"),
    ("market_adjusted_growth",    "DERIVED",     "mom_revenue_growth_pct − market_category_growth_pct", "Brand growth vs category growth", _grain_str, "NULL when either input NULL", "ACTIVE", ""),
    ("sell_out_coverage_pct",     "SELL_OUT",    "coverage_level → numeric", "Store coverage numeric",        _grain_str, "NULL when coverage_level unexpected", "ACTIVE", "HIGH=1.0, MEDIUM/MED=0.75, LOW=0.5. MG29 validates."),
    ("value_share",               "NIELSEN",     "nls_value_share",          "Nielsen value market share",    _grain_str, "NULLABLE",  "ACTIVE",   ""),
    ("volume_share",              "NIELSEN",     "nls_volume_share",         "Nielsen volume market share",   _grain_str, "NULLABLE",  "ACTIVE",   ""),
    ("numeric_distribution",      "NIELSEN",     "nls_numeric_dist",         "Numeric distribution %",        _grain_str, "NULLABLE",  "ACTIVE",   ""),
    ("growth_quality_score",      "DEFERRED",    "NULL",                     "Composite growth quality 0–100","N/A",      "Always NULL","DEFERRED", "W6 — commercial scoring logic not signed off"),
    ("gqs_confidence_level",      "DEFERRED",    "NULL",                     "GQS confidence level",          "N/A",      "Always NULL","DEFERRED", "W6 — same"),
    ("gqs_version",               "DEFERRED",    "NULL",                     "GQS version string",            "N/A",      "Always NULL","DEFERRED", "W6 — same"),
    ("waste_risk_score",          "PHASE_6",     "NULL",                     "Waste risk score 0–1",          "N/A",      "Always NULL","PHASE_6",  "ML model — out of scope"),
    ("competitive_risk_score",    "PHASE_6",     "NULL",                     "Competitive risk score 0–100",  "N/A",      "Always NULL","PHASE_6",  "ML model — out of scope"),
    ("recommended_action",        "PHASE_6",     "NULL",                     "Recommendation engine output",  "N/A",      "Always NULL","PHASE_6",  "ML model — out of scope"),
    ("data_confidence_overall",   "GOLD",        "data_confidence",          "Overall data confidence level", _grain_str, "NOT NULL",  "ACTIVE",   "HIGH/MEDIUM/LOW"),
    ("mart_computed_at",          "AUDIT",       "current_timestamp()",      "Mart build timestamp",          "N/A",      "NOT NULL",  "ACTIVE",   ""),
    ("pipeline_run_id",           "AUDIT",       f"{RUN_ID_SOURCE}",         "Build run identifier",          "N/A",      "NOT NULL",  "ACTIVE",   f"Source: {RUN_ID_SOURCE}"),
]

for m in _METRIC_DEFS:
    register_mart_metric(*m)

flush_mart_registry()
log_mart("INFO", f"Registered {len(_METRIC_DEFS)} mart KPI entries", SECTION)

# COMMAND ----------
# MAGIC %md ## Step 18 — Row count reconciliation + coverage report

# COMMAND ----------

# Row count reconciliation
diff = mart_row_count - gold_row_count
reconcile_lines = [
    f"gold_row_count        : {gold_row_count:,}",
    f"mart_row_count        : {mart_row_count:,}",
    f"difference            : {diff:+,}",
    f"missing_gold_keys     : {'0 (mart_row_count == gold_row_count)' if diff == 0 else 'SEE BELOW'}",
    f"new_mart_keys         : 0",
]
if diff != 0:
    reconcile_lines.append(
        f"documented_exception  : Mart row count differs by {diff:+,}. "
        "Review mart_validation.py MG1A for details."
    )
with open(RECONCILE_PATH, "w") as f:
    f.write("\n".join(reconcile_lines) + "\n")
log_mart("INFO", f"Row count reconciliation written → {RECONCILE_PATH}", SECTION)

# Coverage report
has_si  = df_mart.filter(F.col("sell_in_units").isNotNull()).count()
has_so  = df_mart.filter(F.col("sell_out_revenue").isNotNull()).count()
has_inv = df_mart.filter(F.col("investment_spend").isNotNull()).count()
has_nls = df_mart.filter(F.col("value_share").isNotNull() | F.col("volume_share").isNotNull()).count()
has_all = df_mart.filter(
    F.col("sell_in_units").isNotNull() &
    F.col("sell_out_revenue").isNotNull() &
    F.col("investment_spend").isNotNull() &
    (F.col("value_share").isNotNull() | F.col("volume_share").isNotNull())
).count()
sparse_mom = df_mart.filter(F.col("mom_revenue_growth_pct").isNull()).count()

def pct(n, total): return f"{n / total * 100:.1f}%" if total > 0 else "N/A"

coverage_lines = [
    f"rows_with_sell_in           : {has_si:,} ({pct(has_si, mart_row_count)})",
    f"rows_with_sell_out          : {has_so:,} ({pct(has_so, mart_row_count)})",
    f"rows_with_investment        : {has_inv:,} ({pct(has_inv, mart_row_count)})",
    f"rows_with_nielsen           : {has_nls:,} ({pct(has_nls, mart_row_count)})",
    f"rows_with_all_sources       : {has_all:,} ({pct(has_all, mart_row_count)})",
    f"sparse_mom_rows             : {sparse_mom:,} (first month per grain — no prior period, MoM=NULL)",
    "sell_in_units_proxy_warning : sell_in_units mapped from si_vol_litros (liters, not discrete units) — MG28",
]
with open(COVERAGE_PATH, "w") as f:
    f.write("\n".join(coverage_lines) + "\n")
log_mart("INFO", f"Coverage report written → {COVERAGE_PATH}", SECTION)

# COMMAND ----------
# MAGIC %md ## Step 19 — Write audit log

# COMMAND ----------

write_mart_audit_log(
    pipeline_run_id   = PIPELINE_RUN_ID,
    run_id_source     = RUN_ID_SOURCE,
    gold_rows         = gold_row_count,
    mart_rows         = mart_row_count,
    validation_status = "PENDING — run mart_validation.py for gate",
)

log_mart("INFO", "=" * 72, SECTION)
log_mart("INFO", "GOLD_COMMERCIAL_KPI MART BUILDER — COMPLETE ✅", SECTION)
log_mart("INFO", f"Rows: Gold={gold_row_count:,} | Mart={mart_row_count:,}", SECTION)
log_mart("INFO", f"DBFS: {DBFS_MART_TABLE}", SECTION)
log_mart("INFO", f"CSV : {PBI_CSV_PATH}", SECTION)
log_mart("INFO", "Run mart_validation.py next → gate check", SECTION)
log_mart("INFO", "=" * 72, SECTION)
