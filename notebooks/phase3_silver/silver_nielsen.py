# Databricks notebook source
# =============================================================================
# PHASE 3 — NIELSEN STANDARDIZATION: silver_nielsen.py
# =============================================================================
# CONFIRMED SCHEMA (from column_types_snapshot.yaml):
#   AGG_DATA_PVT : market_id (NUMBER), period_id, product_id, FACT_COLUMN, FACT_VALUE
#   MKT_DIM      : market_id (NUMBER), MRKT_DSC_SHRT (TEXT), hierarchy_level
#   PER_DIM      : period_id, period_short_description, period_long_description, period_ending_datetime
#   PROD_DIM     : product_id, [brand columns per CBU]
#   FACT_REF     : fact_column, fact_description, fact_group, fact_subgroup
#
# JOIN KEY: AGG_DATA_PVT.market_id = MKT_DIM.market_id  (NOT MRKT_DSC_SHRT)
# MRKT_DSC_SHRT lives ONLY in MKT_DIM — never in AGG_DATA_PVT.
#
# ⚠️  SCALE DESIGN — 938M+ ROW FACT TABLES:
#   ALL joins pushed into Snowflake SQL. Only aggregated result pulled to Spark.
#   Pattern: Snowflake handles volume → Spark handles mapping standardization.
#
# 3-STEP DESIGN (R13):
#   Step A: Build unique market dim from MKT_DIM tables (small — hundreds of rows)
#   Step B: Apply M1 mapping (canal_std / region_std) — Spark on small dim
#   Step C: Snowflake-pushdown aggregation per CBU joining AGG_DATA × MKT_DIM
#           Returns aggregated fact per (MRKT_DSC_SHRT × product × period) — small
#
# RULES: R13, R14, R15
# =============================================================================

# COMMAND ----------

# MAGIC %run ./silver_homologation_apply

# COMMAND ----------

# MAGIC %md
# MAGIC ## NIELSEN Standardization — silver_nielsen.py
# MAGIC **Confirmed schema:** AGG_DATA_PVT joins to MKT_DIM on `market_id`, not `MRKT_DSC_SHRT`.
# MAGIC **Scale pattern:** 938M+ rows aggregated in Snowflake. Spark receives small result.

# COMMAND ----------

_S = "NIELSEN"
log("INFO", "Starting NIELSEN standardization", _S)
log("INFO", "AGG_DATA_PVT join key: market_id = MKT_DIM.market_id", _S)
log("INFO", "MRKT_DSC_SHRT lives in MKT_DIM only — never in AGG_DATA_PVT", _S)

# Confirmed table registry — exact names from pipeline_config.yaml (Change #6)
_CBU_TABLES = {
    "EDP_NIELSEN": {
        "agg":  "PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_AGG_DATA_PVT",
        "mkt":  "PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_MKT_DIM",
        "per":  "PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_PER_DIM",
        "prod": "PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_PROD_DIM",
        "ref":  "PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_FACT_REF",
    },
    "PB_NIELSEN": {
        "agg":  "PRD_MEX.MEX_DSP_DPH_MKT.VW_SUST_LECHE_ST_NLSN_AGG_DATA_PVT",
        "mkt":  "PRD_MEX.MEX_DSP_DPH_MKT.VW_SUST_LECHE_ST_NLSN_MKT_DIM",
        "per":  "PRD_MEX.MEX_DSP_DPH_MKT.VW_SUST_LECHE_ST_NLSN_PER_DIM",
        "prod": "PRD_MEX.MEX_DSP_DPH_MKT.VW_SUST_LECHE_ST_NLSN_PROD_DIM",
        "ref":  "PRD_MEX.MEX_DSP_DPH_MKT.VW_SUST_LECHE_ST_NLSN_FACT_REF",
    },
    "WATER_RETAIL": {
        "agg":  "PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_RT_NLSN_AGG_DATA_PVT",
        "mkt":  "PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_RT_NLSN_MKT_DIM",
        "per":  "PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_RT_NLSN_PER_DIM",
        "prod": "PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_RT_NLSN_PROD_DIM",
        "ref":  "PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_RT_NLSN_FACT_REF",
    },
    "WATER_SCANTRACK": {
        "agg":  "PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_ST_NLSN_AGG_DATA_PVT",
        "mkt":  "PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_ST_NLSN_MKT_DIM",
        "per":  "PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_ST_NLSN_PER_DIM",
        "prod": "PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_ST_NLSN_PROD_DIM",
        "ref":  "PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_ST_NLSN_FACT_REF",
    },
}

# COMMAND ----------

# DBTITLE 1,Cell 5
# =============================================================================
# STEP A — Build unique market dimension (small — hundreds of rows)
# Pull from MKT_DIM tables which are tiny. Join key is market_id.
# =============================================================================
log("INFO", "Step A: Building unique market dimension from MKT_DIM tables", _S)

_mkt_frames = []
for cbu_label, tbls in _CBU_TABLES.items():
    try:
        # MKT_DIM is small (hundreds of rows) — safe to pull into Spark
        df_mkt = run_sf(DB_PRD_MEX, f"""
            SELECT DISTINCT
                market_id,
                MRKT_DSC_SHRT,
                hierarchy_level,
                hierarchy_number
            FROM {tbls['mkt']}
            WHERE MRKT_DSC_SHRT IS NOT NULL
        """)
        df_mkt = df_mkt.withColumn("nielsen_source_cbu", F.lit(cbu_label))
        n = df_mkt.count()
        log("INFO", f"{cbu_label} MKT_DIM: {n:,} market entries", _S)
        _mkt_frames.append(df_mkt)
    except Exception as e:
        blocker(True, f"Cannot read MKT_DIM for {cbu_label}: {e}", _S)

if not _mkt_frames:
    blocker(True, "All 4 MKT_DIM tables failed — cannot build nielsen_std", _S)
    raise RuntimeError("All 4 MKT_DIM tables failed — cannot build nielsen_std")

from functools import reduce as _reduce
df_mkt_raw = _reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), _mkt_frames)
n_raw = df_mkt_raw.count()

# R13: deduplicate to 1 row per MRKT_DSC_SHRT
df_nielsen_dim = df_mkt_raw.dropDuplicates(["MRKT_DSC_SHRT"])
n_dim = df_nielsen_dim.count()
log("INFO",
    f"Nielsen market dim: {n_dim:,} unique MRKT_DSC_SHRT "
    f"({n_raw - n_dim:,} cross-CBU duplicates removed, R13)", _S)

# Assert uniqueness (R14)
dup_mkt = df_nielsen_dim.count() - df_nielsen_dim.dropDuplicates(["MRKT_DSC_SHRT"]).count()
blocker(dup_mkt > 0,
    f"Market dim still has {dup_mkt} duplicate MRKT_DSC_SHRT after dedup.", _S)

# COMMAND ----------

# =============================================================================
# STEP B — Apply M1 mapping (canal_std / region_std) — Spark on small dim
# M1 source: logs/signoff_03_nielsen_markets.csv (362 rows — authoritative)
# NOT homologation/nielsen_market_mapping.csv (11 rows — partial predecessor)
# =============================================================================
log("INFO", "Step B: Applying M1 market mapping", _S)

df_m1 = load_mapping_csv(
    "logs/signoff_03_nielsen_markets.csv",
    key_col="mrkt_dsc_shrt", section=_S)

# Check if canal_std / region_std columns exist in M1 file (may be pending)
m1_cols_lower = [c.lower() for c in df_m1.columns] if df_m1.columns else []
has_canal_std  = "canal_std"  in m1_cols_lower
has_region_std = "region_std" in m1_cols_lower
has_status     = any(c in m1_cols_lower for c in ["mapping_status", "review_status"])

log("INFO",
    f"M1 columns present — canal_std: {has_canal_std}, "
    f"region_std: {has_region_std}, status: {has_status}", _S)

# Build select list based on what is actually in the M1 file
m1_select = [F.col("mrkt_dsc_shrt").alias("m1_key")]
if has_canal_std:
    m1_select.append(F.col("canal_std"))
else:
    m1_select.append(F.lit(None).cast("string").alias("canal_std"))
    warn(True, "M1 PENDING: canal_std column not yet in signoff_03 CSV", _S)
if has_region_std:
    m1_select.append(F.col("region_std"))
else:
    m1_select.append(F.lit(None).cast("string").alias("region_std"))
    warn(True, "M1 PENDING: region_std column not yet in signoff_03 CSV", _S)
if has_status:
    status_col = "mapping_status" if "mapping_status" in m1_cols_lower else "review_status"
    m1_select.append(F.col(status_col).alias("mapping_status"))
else:
    m1_select.append(F.lit("NEEDS_REVIEW").alias("mapping_status"))

df_m1_sel = df_m1.select(m1_select)

df_nielsen_std = df_nielsen_dim.join(
    df_m1_sel,
    df_nielsen_dim["MRKT_DSC_SHRT"] == df_m1_sel["m1_key"],
    "left")
register_join("silver_nielsen", "nielsen_market_dim", "signoff_03_nielsen_markets",
              "MRKT_DSC_SHRT=mrkt_dsc_shrt", "left")
assert_row_count_exact(df_nielsen_dim, df_nielsen_std,
                       "nielsen_std x M1 market mapping", _S)

# Quarantine NEEDS_REVIEW
df_nr = df_nielsen_std.filter(
    F.col("mapping_status").isNull() | (F.col("mapping_status") == "NEEDS_REVIEW"))
quarantine(df_nr, "NIELSEN_MARKET_NEEDS_REVIEW",
           "M1 PENDING: market string not yet mapped to canal_std/region_std", _S)

n_confirmed = df_nielsen_std.filter(F.col("mapping_status") == "CONFIRMED").count()
coverage = round(n_confirmed / n_dim * 100, 1) if n_dim > 0 else 0.0
log("INFO", f"M1 mapping: {n_confirmed:,} CONFIRMED | {df_nr.count():,} NEEDS_REVIEW "
    f"({coverage}% coverage)", _S)
warn(coverage < 50.0, f"Nielsen coverage {coverage}% below 50% threshold. Complete M1.", _S)

save_df(df_nielsen_std, "nielsen_std.csv", _S)
log("INFO", f"nielsen_std saved: {n_dim:,} unique market strings", _S)

# COMMAND ----------

# =============================================================================
# STEP C — Snowflake-pushdown aggregation per CBU
# Join AGG_DATA x MKT_DIM x PER_DIM IN SNOWFLAKE.
# 938M+ rows aggregated in Snowflake — only small result set returned to Spark.
# =============================================================================
log("INFO", "Step C: Snowflake-pushdown aggregation for each CBU fact table", _S)
log("INFO", "AGG_DATA_PVT has 938M+ rows — aggregation runs inside Snowflake.", _S)

_fact_frames_std = []
for cbu_label, tbls in _CBU_TABLES.items():
    try:
        log("INFO", f"Executing Snowflake pushdown for {cbu_label}...", _S)

        sql_cbu = f"""
        SELECT
            m.MRKT_DSC_SHRT,
            m.hierarchy_level,
            p.period_short_description  AS period_label,
            p.period_ending_datetime    AS period_end_date,
            a.FACT_COLUMN               AS metric_name,
            r.fact_description          AS metric_description,
            r.fact_group                AS metric_group,
            SUM(COALESCE(a.FACT_VALUE, 0)) AS metric_value,
            COUNT(*)                    AS row_count

        FROM {tbls['agg']} a

        -- Join market dim on market_id (confirmed key — NOT MRKT_DSC_SHRT)
        LEFT JOIN {tbls['mkt']} m
            ON a.market_id = m.market_id

        -- Join period dim on period_id
        LEFT JOIN {tbls['per']} p
            ON a.period_id = p.period_id

        -- Join fact reference for metric descriptions
        LEFT JOIN {tbls['ref']} r
            ON UPPER(a.FACT_COLUMN) = UPPER(r.fact_column)

        WHERE m.MRKT_DSC_SHRT IS NOT NULL

        GROUP BY
            m.MRKT_DSC_SHRT,
            m.hierarchy_level,
            p.period_short_description,
            p.period_ending_datetime,
            a.FACT_COLUMN,
            r.fact_description,
            r.fact_group
        """

        df_cbu = run_sf(DB_PRD_MEX, sql_cbu)
        df_cbu.cache()
        n_cbu = df_cbu.count()
        log("INFO", f"{cbu_label}: {n_cbu:,} aggregated rows returned from Snowflake", _S)

        # Join to nielsen_std for canal_std / region_std (Spark — both small)
        df_cbu_std = df_cbu.join(
            df_nielsen_std.select(
                F.col("MRKT_DSC_SHRT"),
                F.col("canal_std"),
                F.col("region_std"),
                F.col("mapping_status").alias("market_mapping_status")),
            on="MRKT_DSC_SHRT",
            how="left")
        register_join("silver_nielsen", f"{cbu_label}_agg", "nielsen_std",
                      "MRKT_DSC_SHRT", "left")
        assert_row_count_exact(df_cbu, df_cbu_std,
                               f"{cbu_label} agg x nielsen_std", _S)

        df_cbu_std = (df_cbu_std
                      .withColumn("cbu_source",     F.lit(cbu_label))
                      .withColumn("source_system",  F.lit("NIELSEN"))
                      .withColumn("std_created_at", F.current_timestamp()))
        _fact_frames_std.append(df_cbu_std)

    except Exception as e:
        warn(True, f"Cannot process {cbu_label}: {e}", _S)

if _fact_frames_std:
    df_nielsen_facts_std = _reduce(
        lambda a, b: a.unionByName(b, allowMissingColumns=True), _fact_frames_std)
    n_total = df_nielsen_facts_std.count()
    log("INFO", f"nielsen_facts_std: {n_total:,} rows across {len(_fact_frames_std)} CBUs", _S)
    save_df(df_nielsen_facts_std, "nielsen_facts_std.csv", _S)
else:
    blocker(True, "No CBU fact tables produced output — nielsen_facts_std empty", _S)

flush_log("phase3_standardization_audit_log.txt")
log("INFO", "NIELSEN standardization complete.", _S)

# COMMAND ----------


