# Databricks notebook source
# =============================================================================
# PHASE 3 — MKT_OFF STANDARDIZATION: silver_mkt_off_std.py
# =============================================================================
# CONFIRMED SCHEMA (same as MKT_ON — same Nielsen source tables):
#   AGG_DATA_PVT.market_id → MKT_DIM.market_id → MKT_DIM.MRKT_DSC_SHRT
#
# ⚠️  938M+ ROW TABLES: Snowflake-pushdown aggregation.
#
# RULES:
#   R7:  MKT_OFF NEVER joins by UPC
#   R8:  MKT_OFF NEVER joins by CADENA
#   R9:  cadena_std = NULL — approved structural hardcoding (not R12 violation)
#   R13: Join to nielsen_std (unique) — never raw MKT_DIM
#   R14/R15: Uniqueness + row count assertions
#
# ASSERTIONS: A2 (R7 no UPC), A3 (R8 no CADENA), A4 (cadena_std 100% NULL)
# =============================================================================

# COMMAND ----------

# MAGIC %run ./silver_homologation_apply

# COMMAND ----------

# MAGIC %md
# MAGIC ## MKT_OFF Standardization — silver_mkt_off_std.py
# MAGIC **`cadena_std` is intentionally NULL** — approved structural hardcoding (R9).
# MAGIC This is an architectural contract, not a data quality gap.

# COMMAND ----------

_S = "MKT_OFF"
log("INFO", "Starting MKT_OFF standardization (Snowflake-pushdown pattern)", _S)
log("INFO", "cadena_std = NULL is an architectural contract (R9) — not a gap", _S)

import os as _os

# Load nielsen_std (unique on MRKT_DSC_SHRT — R13)
_nielsen_path = _os.path.join(REPO_LOGS_DIR, "nielsen_std.csv")
if not _os.path.exists(_nielsen_path):
    blocker(True,
        f"nielsen_std.csv not found at {_nielsen_path}. "
        "Run silver_nielsen.py before silver_mkt_off_std.py.", _S)
    df_nielsen_std = None
else:
    df_nielsen_std = spark.read.option("header", "true").csv(f"file://{_nielsen_path}")
    dup_ns = df_nielsen_std.count() - df_nielsen_std.dropDuplicates(["MRKT_DSC_SHRT"]).count()
    blocker(dup_ns > 0,
        f"nielsen_std has {dup_ns} duplicate MRKT_DSC_SHRT — cannot join safely.", _S)
    log("INFO", f"nielsen_std loaded: {df_nielsen_std.count():,} unique market strings", _S)

# COMMAND ----------

_MKT_OFF_TABLES = {
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

_mkt_off_frames = []
for cbu_label, tbls in _MKT_OFF_TABLES.items():
    try:
        log("INFO", f"Snowflake pushdown for MKT_OFF {cbu_label}...", _S)

        # Same pushdown query as MKT_ON — join on market_id, not MRKT_DSC_SHRT
        sql_mkt_off = f"""
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

        df_off = run_sf(DB_PRD_MEX, sql_mkt_off)
        df_off.cache()
        n_off = df_off.count()
        log("INFO", f"{cbu_label}: {n_off:,} aggregated rows from Snowflake", _S)

        # Join to nielsen_std (Spark — both small)
        if df_nielsen_std is not None:
            df_off = df_off.join(
                df_nielsen_std.select(
                    F.col("MRKT_DSC_SHRT"),
                    F.col("canal_std"),
                    F.col("region_std"),
                    F.col("mapping_status").alias("market_mapping_status")),
                on="MRKT_DSC_SHRT",
                how="left")
            register_join("silver_mkt_off_std", f"{cbu_label}_agg", "nielsen_std",
                          "MRKT_DSC_SHRT", "left")

        # R9: cadena_std IS INTENTIONALLY NULL — approved structural hardcoding (Change #10)
        # MKT_OFF never has chain-level data by design.
        df_off = df_off.withColumn("cadena_std", F.lit(None).cast(StringType()))

        df_off = (df_off
                  .withColumn("cbu_source",    F.lit(cbu_label))
                  .withColumn("source_system", F.lit("MKT_OFF"))
                  .withColumn("std_created_at", F.current_timestamp()))
        _mkt_off_frames.append(df_off)

    except Exception as e:
        warn(True, f"Cannot process MKT_OFF {cbu_label}: {e}", _S)

if _mkt_off_frames:
    df_mkt_off_std = _reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True),
                             _mkt_off_frames)
    n_mkt_off = df_mkt_off_std.count()
    log("INFO", f"mkt_off_std: {n_mkt_off:,} total rows", _S)
    save_df(df_mkt_off_std, "mkt_off_std.csv", _S)
else:
    blocker(True, "No MKT_OFF CBUs produced output — mkt_off_std empty", _S)
    df_mkt_off_std = None
    n_mkt_off = 0

# COMMAND ----------

# A4: cadena_std must be NULL for every row in mkt_off_std (R9)
if df_mkt_off_std is not None:
    n_non_null = df_mkt_off_std.filter(F.col("cadena_std").isNotNull()).count()
    blocker(n_non_null > 0,
        f"A4 VIOLATION: {n_non_null:,} rows in mkt_off_std have non-NULL cadena_std. "
        "cadena_std must be NULL by design (R9).", _S)
    if n_non_null == 0:
        passed(f"A4: cadena_std is NULL for all {n_mkt_off:,} mkt_off_std rows (R9 confirmed)", _S)

# COMMAND ----------

# A2/A3: Scan JOIN_REGISTRY for prohibited join keys in MKT_OFF entries
_backup = JOIN_REGISTRY.copy()
_mkt_off_entries = [e for e in JOIN_REGISTRY if e["notebook"] == "silver_mkt_off_std"]
JOIN_REGISTRY.clear()
JOIN_REGISTRY.extend(_mkt_off_entries)

# A2: R7 — MKT_OFF no UPC join
assert_no_prohibited_join(
    prohibited_keys=["UPC", "EAN", "SKU_EAN", "SKU_EAN_COD", "INT_ID"],
    rule_label="A2 — R7: MKT_OFF no UPC join", section=_S)

# A3: R8 — MKT_OFF no CADENA join
assert_no_prohibited_join(
    prohibited_keys=["CADENA", "cadena_std", "CUS_CADENA"],
    rule_label="A3 — R8: MKT_OFF no CADENA join", section=_S)

JOIN_REGISTRY.clear()
JOIN_REGISTRY.extend(_backup)

flush_log("phase3_standardization_audit_log.txt")
log("INFO", "MKT_OFF standardization complete.", _S)

# COMMAND ----------

