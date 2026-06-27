# Databricks notebook source
# =============================================================================
# PHASE 3 — NIELSEN STANDARDIZATION: silver_nielsen.py
# =============================================================================
# CONFIRMED SCHEMA (column_types_snapshot.yaml + SEMANTIC_LAYOUTS):
#
#   AGG_DATA_PVT columns (all CASE-SENSITIVE — double-quoted in SQL):
#     "market_id"   NUMBER(38,0)    ← join key to MKT_DIM
#     "product_id"  NUMBER(38,0)
#     "period_id"   NUMBER(38,0)
#     FACT_COLUMN   TEXT            ← unquoted / uppercase
#     FACT_VALUE    NUMBER(38,9)    ← unquoted / uppercase
#
#   MKT_DIM columns:
#     "market_id"       NUMBER(38,0) ← join key FROM AGG_DATA_PVT
#     MRKT_DSC_SHRT     TEXT         ← unquoted / uppercase
#     "hierarchy_level" NUMBER(38,0) ← double-quoted lowercase
#     "hierarchy_number" NUMBER(38,0)
#     "hierarchy_name"   TEXT
#     "hierarchy_column" TEXT
#
#   PER_DIM columns:
#     "period_id"              NUMBER(38,0) ← join key
#     "period_ending_datetime" (TEXT / TIMESTAMP) ← double-quoted
#     period_short_description TEXT
#     period_long_description  TEXT
#
#   FACT_REF columns:
#     "fact_column"     TEXT  ← double-quoted lowercase
#     fact_description  TEXT
#     fact_group        TEXT
#     fact_subgroup     TEXT
#
# CANONICAL JOIN PATTERN (from SEMANTIC_LAYOUTS/*/EDP_MARKET.txt etc.):
#   AGG_DATA_PVT AS J
#     JOIN MKT_DIM  AS K  ON J."market_id" = K."market_id"
#     JOIN PER_DIM  AS L  ON L."period_id" = J."period_id"
#     JOIN FACT_REF AS D  ON J.FACT_COLUMN = D."fact_column"     ← EDP/PB/WATER_SCANTRACK
#                        OR J."FACT_COLUMN" = REPLACE(D."fact_column", '.', '_')  ← WATER_RETAIL only
#
# ⚠️  938M+ ROW TABLES: All joins + GROUP BY pushed into Snowflake SQL.
#     Spark receives only the aggregated result (small).
#
# RULES: R13, R14, R15
# =============================================================================

# COMMAND ----------

# MAGIC %run ./silver_homologation_apply

# COMMAND ----------

# MAGIC %md
# MAGIC ## NIELSEN Standardization — silver_nielsen.py
# MAGIC **Key:** All lowercase columns (`"market_id"`, `"period_id"` etc.) are **double-quoted** in SQL.
# MAGIC `MRKT_DSC_SHRT` and `FACT_COLUMN` are unquoted uppercase.

# COMMAND ----------

_S = "NIELSEN"
log("INFO", "Starting NIELSEN standardization", _S)
log("INFO", "Join key: J.\"market_id\" = K.\"market_id\" (double-quoted — case-sensitive)", _S)

from functools import reduce as _reduce

# Confirmed table registry (exact names from pipeline_config.yaml)
# FACT_REF join differs for WATER_RETAIL (uses REPLACE to strip dots)
_CBU_TABLES = {
    "EDP_NIELSEN": {
        "agg":  "PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_AGG_DATA_PVT",
        "mkt":  "PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_MKT_DIM",
        "per":  "PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_PER_DIM",
        "ref":  "PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_FACT_REF",
        "fact_join": "J.FACT_COLUMN = D.\"fact_column\"",       # standard join
    },
    "PB_NIELSEN": {
        "agg":  "PRD_MEX.MEX_DSP_DPH_MKT.VW_SUST_LECHE_ST_NLSN_AGG_DATA_PVT",
        "mkt":  "PRD_MEX.MEX_DSP_DPH_MKT.VW_SUST_LECHE_ST_NLSN_MKT_DIM",
        "per":  "PRD_MEX.MEX_DSP_DPH_MKT.VW_SUST_LECHE_ST_NLSN_PER_DIM",
        "ref":  "PRD_MEX.MEX_DSP_DPH_MKT.VW_SUST_LECHE_ST_NLSN_FACT_REF",
        "fact_join": "J.FACT_COLUMN = D.\"fact_column\"",       # standard join
    },
    "WATER_RETAIL": {
        "agg":  "PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_RT_NLSN_AGG_DATA_PVT",
        "mkt":  "PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_RT_NLSN_MKT_DIM",
        "per":  "PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_RT_NLSN_PER_DIM",
        "ref":  "PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_RT_NLSN_FACT_REF",
        # CRITICAL: WATER_RETAIL uses REPLACE to strip dots from fact_column names
        "fact_join": "J.\"FACT_COLUMN\" = REPLACE(D.\"fact_column\", '.', '_')",
    },
    "WATER_SCANTRACK": {
        "agg":  "PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_ST_NLSN_AGG_DATA_PVT",
        "mkt":  "PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_ST_NLSN_MKT_DIM",
        "per":  "PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_ST_NLSN_PER_DIM",
        "ref":  "PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_ST_NLSN_FACT_REF",
        "fact_join": "J.FACT_COLUMN = D.\"fact_column\"",       # standard join
    },
}

# COMMAND ----------

# =============================================================================
# STEP A — Build unique market dimension from MKT_DIM tables (small)
# Double-quote "market_id" and "hierarchy_level" — they are case-sensitive lowercase.
# MRKT_DSC_SHRT is unquoted uppercase.
# =============================================================================
log("INFO", "Step A: Building unique market dimension from MKT_DIM tables", _S)

_mkt_frames = []
for cbu_label, tbls in _CBU_TABLES.items():
    try:
        # MKT_DIM is small (hundreds of rows) — safe to pull into Spark
        # "market_id" and "hierarchy_level" must be double-quoted (case-sensitive)
        df_mkt = run_sf(DB_PRD_MEX, f"""
            SELECT DISTINCT
                "market_id",
                MRKT_DSC_SHRT,
                "hierarchy_level",
                "hierarchy_number",
                "hierarchy_name",
                "hierarchy_column"
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
    raise RuntimeError("All MKT_DIM reads failed — cannot continue.")

df_mkt_raw = _reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), _mkt_frames)
n_raw = df_mkt_raw.count()

# R13: deduplicate to 1 row per MRKT_DSC_SHRT (across CBUs)
df_nielsen_dim = df_mkt_raw.dropDuplicates(["MRKT_DSC_SHRT"])
n_dim = df_nielsen_dim.count()
log("INFO",
    f"Nielsen market dim: {n_dim:,} unique MRKT_DSC_SHRT "
    f"({n_raw - n_dim:,} cross-CBU duplicates removed, R13)", _S)

# R14: Assert uniqueness
dup_mkt = n_dim - df_nielsen_dim.dropDuplicates(["MRKT_DSC_SHRT"]).count()
blocker(dup_mkt > 0,
    f"Market dim still has {dup_mkt} duplicate MRKT_DSC_SHRT after dedup.", _S)

# COMMAND ----------

# =============================================================================
# STEP B — Apply M1 mapping (canal_std / region_std) — Spark on small dim
# =============================================================================
log("INFO", "Step B: Applying M1 market mapping (signoff_03_nielsen_markets.csv)", _S)

# ── DEFINITIVE FIX for region_std ambiguity ─────────────────────────────────
# signoff_03 CSV has two region columns at different positions:
#   col[7]  REGION_STD  — uppercase, empty (original Snowflake signoff output)
#   col[10] region_std  — lowercase, M1-populated values (AREA_1, ACAPULCO…)
# Spark's case-insensitive resolution always picks the FIRST match (col7=empty).
# df.drop("REGION_STD") is also case-insensitive and drops BOTH columns.
# Solution: load via pandas (case-sensitive), select cols by exact name, convert to Spark.
import pandas as pd
_m1_repo_path = os.path.join(REPO_ROOT, "logs", "signoff_03_nielsen_markets.csv")
_pdf_m1 = pd.read_csv(_m1_repo_path, dtype=str).fillna("")

# Select only the columns we need by their exact lowercase/uppercase name
_keep_cols = {
    "mrkt_key":      _pdf_m1["MRKT_DSC_SHRT"].str.strip().str.upper(),
    "canal_std_m1":  _pdf_m1["canal_std"].str.strip(),
    "region_std_m1": _pdf_m1["region_std"].str.strip(),   # col[10] — explicit, unambiguous
    "mapping_status": _pdf_m1["REVIEW_STATUS"].str.strip(),
}
_pdf_m1_sel = pd.DataFrame(_keep_cols)

# Convert to Spark — small table (362 rows), safe to collect
df_m1_sel = spark.createDataFrame(_pdf_m1_sel)

n_m1 = df_m1_sel.count()
log("INFO", f"M1 mapping loaded via pandas: {n_m1} rows (bypasses Spark case-insensitive col resolution)", _S)

# Sanity check — region_std_m1 should be non-empty for most rows
n_region_filled = df_m1_sel.filter(F.col("region_std_m1") != "").count()
log("INFO", f"M1 region_std_m1 non-empty: {n_region_filled}/{n_m1}", _S)
warn(n_region_filled == 0, "M1 PENDING: region_std_m1 is empty for all rows — check signoff_03 CSV col[10]", _S)

# canal_std_m1 sanity check
n_canal_filled = df_m1_sel.filter(F.col("canal_std_m1") != "").count()
log("INFO", f"M1 canal_std_m1 non-empty: {n_canal_filled}/{n_m1}", _S)



df_nielsen_std = df_nielsen_dim.join(
    df_m1_sel,
    F.upper(F.trim(df_nielsen_dim["MRKT_DSC_SHRT"])) == df_m1_sel["mrkt_key"],  # mrkt_key = UPPER TRIM from pandas load
    "left")
register_join("silver_nielsen", "nielsen_market_dim", "signoff_03_nielsen_markets",
              "MRKT_DSC_SHRT=mrkt_dsc_shrt", "left")
assert_row_count_exact(df_nielsen_dim, df_nielsen_std,
                       "nielsen_std x M1 market mapping", _S)

# Resolve _m1 aliases back to canonical column names (avoids ambiguity with dim frame columns)
df_nielsen_std = df_nielsen_std \
    .withColumn("canal_std",  F.col("canal_std_m1")) \
    .withColumn("region_std", F.col("region_std_m1"))

df_nr = df_nielsen_std.filter(
    F.col("mapping_status").isNull() | (F.col("mapping_status") == "NEEDS_REVIEW"))
quarantine(df_nr, "NIELSEN_MARKET_NEEDS_REVIEW",
           "M1 PENDING: market string not mapped to canal_std/region_std", _S)

n_confirmed = df_nielsen_std.filter(F.col("mapping_status") == "CONFIRMED").count()
coverage = round(n_confirmed / n_dim * 100, 1) if n_dim > 0 else 0.0
log("INFO", f"M1 coverage: {n_confirmed:,} CONFIRMED / {n_dim:,} total ({coverage}%)", _S)
warn(coverage < 50.0, f"Nielsen M1 coverage {coverage}% < 50% — complete the mapping CSV", _S)

save_df(df_nielsen_std, "nielsen_std.csv", _S)
log("INFO", f"nielsen_std saved: {n_dim:,} rows", _S)

# COMMAND ----------

# =============================================================================
# STEP C — Snowflake pushdown aggregation per CBU
# ALL joins run inside Snowflake. Spark receives only aggregated result.
# "market_id", "period_id" must be double-quoted in SQL.
# WATER_RETAIL uses REPLACE(D."fact_column", '.', '_') for FACT_REF join.
# =============================================================================
log("INFO", "Step C: Snowflake-pushdown aggregation per CBU (938M+ rows in Snowflake)", _S)

_fact_frames = []
for cbu_label, tbls in _CBU_TABLES.items():
    try:
        log("INFO", f"Executing Snowflake pushdown for {cbu_label}...", _S)

        fact_join_clause = tbls["fact_join"]

        sql_cbu = f"""
        SELECT
            K.MRKT_DSC_SHRT,
            K."hierarchy_level",
            L."period_short_description"  AS period_label,
            L."period_ending_datetime"    AS period_end_date,
            J.FACT_COLUMN                 AS metric_name,
            D."fact_description"          AS metric_description,
            D."fact_group"                AS metric_group,
            SUM(COALESCE(J.FACT_VALUE, 0)) AS metric_value,
            COUNT(*)                       AS row_count

        FROM {tbls['agg']} AS J

        -- market join: "market_id" double-quoted (case-sensitive lowercase)
        LEFT JOIN {tbls['mkt']} AS K
            ON J."market_id" = K."market_id"

        -- period join: "period_id" double-quoted (case-sensitive lowercase)
        LEFT JOIN {tbls['per']} AS L
            ON L."period_id" = J."period_id"

        -- fact reference join (WATER_RETAIL uses REPLACE for dots)
        LEFT JOIN {tbls['ref']} AS D
            ON {fact_join_clause}

        WHERE K.MRKT_DSC_SHRT IS NOT NULL

        GROUP BY
            K.MRKT_DSC_SHRT,
            K."hierarchy_level",
            L."period_short_description",
            L."period_ending_datetime",
            J.FACT_COLUMN,
            D."fact_description",
            D."fact_group"
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
        _fact_frames.append(df_cbu_std)

    except Exception as e:
        warn(True, f"Cannot process {cbu_label}: {e}", _S)

if _fact_frames:
    df_nielsen_facts_std = _reduce(
        lambda a, b: a.unionByName(b, allowMissingColumns=True), _fact_frames)
    n_total = df_nielsen_facts_std.count()
    log("INFO", f"nielsen_facts_std: {n_total:,} rows across {len(_fact_frames)} CBUs", _S)
    save_df(df_nielsen_facts_std, "nielsen_facts_std.csv", _S)
else:
    blocker(True, "No CBU fact tables produced output — nielsen_facts_std empty", _S)

flush_log("phase3_standardization_audit_log.txt")
log("INFO", "NIELSEN standardization complete.", _S)

# COMMAND ----------

