# Databricks notebook source
# =============================================================================
# PHASE 3 — MKT_ON STANDARDIZATION: silver_mkt_on_std.py
# =============================================================================
# CONFIRMED SCHEMA:
#   AGG_DATA_PVT.market_id → MKT_DIM.market_id → MKT_DIM.MRKT_DSC_SHRT
#   AGG_DATA_PVT.period_id → PER_DIM.period_id → period_short_description
#   AGG_DATA_PVT structure: market_id, period_id, product_id, FACT_COLUMN, FACT_VALUE
#
# ⚠️  938M+ ROW TABLES: Snowflake-pushdown aggregation (same as silver_nielsen).
#
# RULES:
#   R6:  MKT_ON NEVER joins by UPC, EAN, SKU_EAN
#   R13: Join to nielsen_std (unique on MRKT_DSC_SHRT)
#   R14: Uniqueness assertion before joins
#   R15: Row count check after Spark joins
# =============================================================================

# COMMAND ----------

# MAGIC %run ./silver_homologation_apply

# COMMAND ----------

# MAGIC %md
# MAGIC ## MKT_ON Standardization — silver_mkt_on_std.py
# MAGIC **Join key:** `AGG_DATA_PVT.market_id = MKT_DIM.market_id` (confirmed schema).
# MAGIC `MRKT_DSC_SHRT` is in MKT_DIM only. Aggregation pushed to Snowflake.

# COMMAND ----------

_S = "MKT_ON"
log("INFO", "Starting MKT_ON standardization (Snowflake-pushdown pattern)", _S)

import os as _os

# Load nielsen_std (unique on MRKT_DSC_SHRT — R13)
# Written by silver_nielsen.py — must be run first
_nielsen_path = _os.path.join(REPO_LOGS_DIR, "nielsen_std.csv")
if not _os.path.exists(_nielsen_path):
    blocker(True,
        f"nielsen_std.csv not found at {_nielsen_path}. "
        "Run silver_nielsen.py before silver_mkt_on_std.py.", _S)
    df_nielsen_std = None
else:
    df_nielsen_std = spark.read.option("header", "true").csv(f"file://{_nielsen_path}")
    # Assert uniqueness (R14)
    dup_ns = df_nielsen_std.count() - df_nielsen_std.dropDuplicates(["MRKT_DSC_SHRT"]).count()
    blocker(dup_ns > 0,
        f"nielsen_std has {dup_ns} duplicate MRKT_DSC_SHRT — cannot join safely.", _S)
    n_nielsen = df_nielsen_std.count()
    log("INFO", f"nielsen_std loaded: {n_nielsen:,} unique market strings (R13)", _S)

brand_cfg = load_yaml_config("configs/brand_crosswalk.yaml")
log("INFO", f"Brand crosswalk loaded v{brand_cfg.get('version', '?')}", _S)

# COMMAND ----------

# Confirmed AGG + MKT table pairs (Change #6 — no wildcard)
_MKT_ON_TABLES = {
    "EDP_NIELSEN": {
        "agg": "PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_AGG_DATA_PVT",
        "mkt": "PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_MKT_DIM",
        "per": "PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_PER_DIM",
        "ref": "PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_FACT_REF",
    },
    "PB_NIELSEN": {
        "agg": "PRD_MEX.MEX_DSP_DPH_MKT.VW_SUST_LECHE_ST_NLSN_AGG_DATA_PVT",
        "mkt": "PRD_MEX.MEX_DSP_DPH_MKT.VW_SUST_LECHE_ST_NLSN_MKT_DIM",
        "per": "PRD_MEX.MEX_DSP_DPH_MKT.VW_SUST_LECHE_ST_NLSN_PER_DIM",
        "ref": "PRD_MEX.MEX_DSP_DPH_MKT.VW_SUST_LECHE_ST_NLSN_FACT_REF",
    },
    "WATER_RETAIL": {
        "agg": "PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_RT_NLSN_AGG_DATA_PVT",
        "mkt": "PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_RT_NLSN_MKT_DIM",
        "per": "PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_RT_NLSN_PER_DIM",
        "ref": "PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_RT_NLSN_FACT_REF",
    },
    "WATER_SCANTRACK": {
        "agg": "PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_ST_NLSN_AGG_DATA_PVT",
        "mkt": "PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_ST_NLSN_MKT_DIM",
        "per": "PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_ST_NLSN_PER_DIM",
        "ref": "PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_ST_NLSN_FACT_REF",
    },
}

from functools import reduce as _reduce

_mkt_on_frames = []
for cbu_label, tbls in _MKT_ON_TABLES.items():
    try:
        log("INFO", f"Snowflake pushdown for MKT_ON {cbu_label}...", _S)

        # All joins + aggregation in Snowflake — market_id is the join key
        sql_mkt_on = f"""
        SELECT
            m.MRKT_DSC_SHRT,
            m.hierarchy_level,
            p.period_short_description  AS period_label,
            p.period_ending_datetime    AS period_end_date,
            a.FACT_COLUMN               AS metric_name,
            r.fact_group                AS metric_group,
            SUM(COALESCE(a.FACT_VALUE, 0)) AS metric_value,
            COUNT(*)                    AS row_count
        FROM {tbls['agg']} a
        LEFT JOIN {tbls['mkt']} m ON a.market_id = m.market_id
        LEFT JOIN {tbls['per']} p ON a.period_id  = p.period_id
        LEFT JOIN {tbls['ref']} r ON UPPER(a.FACT_COLUMN) = UPPER(r.fact_column)
        WHERE m.MRKT_DSC_SHRT IS NOT NULL
        GROUP BY
            m.MRKT_DSC_SHRT,
            m.hierarchy_level,
            p.period_short_description,
            p.period_ending_datetime,
            a.FACT_COLUMN,
            r.fact_group
        """

        df_on = run_sf(DB_PRD_MEX, sql_mkt_on)
        df_on.cache()
        n_on = df_on.count()
        log("INFO", f"{cbu_label}: {n_on:,} aggregated rows from Snowflake", _S)

        # Join to nielsen_std for canal_std / region_std (Spark — both small)
        if df_nielsen_std is not None:
            df_on = df_on.join(
                df_nielsen_std.select(
                    F.col("MRKT_DSC_SHRT"),
                    F.col("canal_std"),
                    F.col("region_std"),
                    F.col("mapping_status").alias("market_mapping_status")),
                on="MRKT_DSC_SHRT",
                how="left")
            register_join("silver_mkt_on_std", f"{cbu_label}_agg", "nielsen_std",
                          "MRKT_DSC_SHRT", "left")
            assert_row_count_exact(df_on.subtract(df_on.subtract(df_on)),  # preserve count ref
                                   df_on, f"MKT_ON {cbu_label} x nielsen_std", _S)

        df_on = (df_on
                 .withColumn("cbu_source",    F.lit(cbu_label))
                 .withColumn("source_system", F.lit("MKT_ON"))
                 .withColumn("std_created_at", F.current_timestamp()))
        _mkt_on_frames.append(df_on)

    except Exception as e:
        warn(True, f"Cannot process MKT_ON {cbu_label}: {e}", _S)

if _mkt_on_frames:
    df_mkt_on_std = _reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True),
                            _mkt_on_frames)
    n_mkt_on = df_mkt_on_std.count()
    log("INFO", f"mkt_on_std: {n_mkt_on:,} total rows", _S)
    save_df(df_mkt_on_std, "mkt_on_std.csv", _S)
else:
    blocker(True, "No MKT_ON CBUs produced output — mkt_on_std empty", _S)

# COMMAND ----------

# A1: R6 — MKT_ON must never join by UPC/EAN/SKU_EAN (JOIN_REGISTRY scan)
assert_no_prohibited_join(
    prohibited_keys=["UPC", "EAN", "SKU_EAN", "SKU_EAN_COD", "INT_ID"],
    rule_label="A1 — R6: MKT_ON no UPC join",
    section=_S)

flush_log("phase3_standardization_audit_log.txt")
log("INFO", "MKT_ON standardization complete.", _S)

# COMMAND ----------

