# Databricks notebook source
# MAGIC %md
# MAGIC # Semantic Layer — Snowflake Validation Queries
# MAGIC **Purpose**: Run all validation queries against Snowflake and log results.
# MAGIC
# MAGIC Run each cell in order. Results are printed and saved to `/tmp/semantic_validation_log.txt`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — Snowflake Connections

# COMMAND ----------

# ── Snowflake Connection Profiles ────────────────────────────────────────

SF_URL = "danonenam.east-us-2.azure.snowflakecomputing.com"

# ─── Profile: PRD_MEX ─────────────────────────────────────────────────────────
PRD_MEX_PROFILE = {
    "sfURL":       SF_URL,
    "sfUser":      "PRD_OSM_DPH_READER",
    "sfPassword":  "73.bBZmne7Aq",
    "sfWarehouse": "PRD_MEX_ANL_WH",
    "sfRole":      "PRD_MEX_READER",
}

# ─── Profile: PRD_MDP ─────────────────────────────────────────────────────────
# Uses Key Vault credentials via Databricks secret scope
KEYVAULT_SCOPE = "DAN-AM-P-KVT800-R-MDP-DB"
PRD_MDP_PROFILE = {
    "sfURL":       SF_URL,
    "sfUser":      dbutils.secrets.get(scope=KEYVAULT_SCOPE, key="snowflake-user"),
    "sfPassword":  dbutils.secrets.get(scope=KEYVAULT_SCOPE, key="snowflake-password"),
    "sfWarehouse": "PRD_MDP_ANL_WH",
    "sfRole":      "PRD_MDP",
}

# ─── Profile Router ───────────────────────────────────────────────────────────
CONNECTION_PROFILES = {
    "PRD_MEX": PRD_MEX_PROFILE,
    "PRD_MDP": PRD_MDP_PROFILE,
}

def get_sf_options(database: str) -> dict:
    """Return the Snowflake connection options for a given database."""
    if database not in CONNECTION_PROFILES:
        raise ValueError(f"No connection profile for database '{database}'. "
                         f"Available: {list(CONNECTION_PROFILES.keys())}")
    return CONNECTION_PROFILES[database]

# Verify profiles loaded
for db, profile in CONNECTION_PROFILES.items():
    print(f"  {db}: warehouse={profile['sfWarehouse']}, role={profile['sfRole']}, user={profile['sfUser']}")

# COMMAND ----------

import datetime
import os
import io
import sys

LOG_LINES = []

def log(msg):
    """Print and buffer a log line."""
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    LOG_LINES.append(line)

def get_log_path():
    """
    Resolve a writable log path in Databricks notebooks / repos.
    Priority:
      1) current working directory
      2) ./notebooks under cwd (if exists)
      3) /tmp
    """
    try:
        cwd = os.getcwd()
    except Exception:
        cwd = None

    candidates = []

    if cwd:
        # Save in current directory
        candidates.append(os.path.join(cwd, "validation_results.txt"))

        # If a notebooks subfolder exists, use it
        nb_dir = os.path.join(cwd, "notebooks")
        if os.path.isdir(nb_dir):
            candidates.append(os.path.join(nb_dir, "validation_results.txt"))

        # If cwd itself is notebooks, save there directly
        if os.path.basename(cwd).lower() == "notebooks":
            candidates.insert(0, os.path.join(cwd, "validation_results.txt"))

    # Final fallback
    candidates.append("/tmp/validation_results.txt")

    return candidates

def log_df(df, label, n=200):
    """Capture a Spark DataFrame's .show() output into the log."""
    log(f"  {label}:")
    old_stdout = sys.stdout
    buffer = io.StringIO()
    try:
        sys.stdout = buffer
        df.show(n, truncate=False)
    finally:
        sys.stdout = old_stdout

    table_str = buffer.getvalue()
    print(table_str)

    for line in table_str.rstrip().split("\n"):
        LOG_LINES.append(line)

def run_sf_query(database, query, label="query"):
    """Run a Snowflake query via Spark using the profile for the given database."""
    opts = get_sf_options(database)
    log(f"Running: {label}  [db={database}]")
    df = (
        spark.read
        .format("net.snowflake.spark.snowflake")
        .options(**opts)
        .option("sfDatabase", database)
        .option("query", query)
        .load()
    )
    row_count = df.count()
    log(f"  → {row_count} rows returned")
    return df

def save_log():
    """Write accumulated log to the first writable path."""
    last_error = None

    for path in get_log_path():
        try:
            parent = os.path.dirname(path)
            if parent:
                os.makedirs(parent, exist_ok=True)

            with open(path, "w", encoding="utf-8") as f:
                f.write("\n".join(LOG_LINES))

            log(f"Log saved to {path}")
            return path

        except Exception as e:
            last_error = e
            continue

    # If all candidates fail
    log(f"Could not save log to any candidate path. Last error: {last_error}")
    return None

# Header
CANDIDATE_PATHS = get_log_path()
log("=" * 70)
log("SEMANTIC LAYER VALIDATION — Phase B")
log(f"Candidate log paths: {CANDIDATE_PATHS}")
log("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## V1 — SELL_OUT: Monthly Totals (Old vs New Grain)

# COMMAND ----------

# V1A: OLD query (with GROUP BY)
v1a_sql = """
WITH fact_filtered AS (
    SELECT
        PER_ID,
        STORE,
        UPC,
        VOL_SELL_OUT,
        PCS_SELL_OUT,
        AMOUNT_SELL_OUT,
        VOL_INV,
        PCS_INV,
        CBU_ID
    FROM PRD_MDP.MDP_DSP.VW_FACT_SELL_OUT
    WHERE PER_ID >= 20250101
),
base AS (
    SELECT
        per."DAY" AS FECHA,
        st.CHAIN AS CADENA,
        st.FORMAT AS FORMATO_CADENA,
        prod.BRAND AS MARCA,
        f.VOL_SELL_OUT,
        f.PCS_SELL_OUT,
        f.AMOUNT_SELL_OUT,
        f.VOL_INV,
        f.PCS_INV
    FROM fact_filtered f
    INNER JOIN PRD_MDP.MDP_DWH.V_D_PERIOD per
        ON f.PER_ID = per.PER_ID
    INNER JOIN PRD_MDP.MDP_DSP.VW_D_STORE_RM st
        ON f.STORE = st.INT_ID
       AND f.CBU_ID = st.CBU_ID
    INNER JOIN PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM prod
        ON TO_VARCHAR(f.UPC) = TO_VARCHAR(prod.INT_ID)
       AND f.CBU_ID = prod.CBU_ID
),
old_agg AS (
    SELECT
        FECHA,
        CADENA,
        FORMATO_CADENA,
        MARCA,
        SUM(VOL_SELL_OUT) AS VOL_SELL_OUT,
        SUM(PCS_SELL_OUT) AS PCS_SELL_OUT,
        SUM(AMOUNT_SELL_OUT) AS AMOUNT_SELL_OUT,
        SUM(VOL_INV) AS VOL_INV,
        SUM(PCS_INV) AS PCS_INV
    FROM base
    GROUP BY FECHA, CADENA, FORMATO_CADENA, MARCA
)
SELECT
    DATE_TRUNC('MONTH', FECHA) AS MES,
    SUM(VOL_SELL_OUT) AS VOL,
    SUM(PCS_SELL_OUT) AS PCS,
    SUM(AMOUNT_SELL_OUT) AS AMT,
    SUM(VOL_INV) AS V_INV,
    SUM(PCS_INV) AS P_INV,
    COUNT(*) AS ROW_COUNT
FROM old_agg
GROUP BY 1
ORDER BY 1
"""

# V1B: NEW query (without GROUP BY)
v1b_sql = """
WITH fact_filtered AS (
    SELECT
        PER_ID,
        STORE,
        UPC,
        VOL_SELL_OUT,
        PCS_SELL_OUT,
        AMOUNT_SELL_OUT,
        VOL_INV,
        PCS_INV,
        CBU_ID
    FROM PRD_MDP.MDP_DSP.VW_FACT_SELL_OUT
    WHERE PER_ID >= 20250101
),
base AS (
    SELECT
        per."DAY" AS FECHA,
        st.CHAIN AS CADENA,
        st.FORMAT AS FORMATO_CADENA,
        prod.BRAND AS MARCA,
        f.STORE AS STORE_ID,
        TO_VARCHAR(f.UPC) AS UPC,
        st.SUBCHAIN AS SUBCHAIN,
        f.VOL_SELL_OUT,
        f.PCS_SELL_OUT,
        f.AMOUNT_SELL_OUT,
        f.VOL_INV,
        f.PCS_INV
    FROM fact_filtered f
    INNER JOIN PRD_MDP.MDP_DWH.V_D_PERIOD per
        ON f.PER_ID = per.PER_ID
    INNER JOIN PRD_MDP.MDP_DSP.VW_D_STORE_RM st
        ON f.STORE = st.INT_ID
       AND f.CBU_ID = st.CBU_ID
    INNER JOIN PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM prod
        ON TO_VARCHAR(f.UPC) = TO_VARCHAR(prod.INT_ID)
       AND f.CBU_ID = prod.CBU_ID
)
SELECT
    DATE_TRUNC('MONTH', FECHA) AS MES,
    SUM(VOL_SELL_OUT) AS VOL,
    SUM(PCS_SELL_OUT) AS PCS,
    SUM(AMOUNT_SELL_OUT) AS AMT,
    SUM(VOL_INV) AS V_INV,
    SUM(PCS_INV) AS P_INV,
    COUNT(*) AS ROW_COUNT
FROM base
GROUP BY 1
ORDER BY 1
"""

log("V1: SELL_OUT totals comparison")
df_old = run_sf_query("PRD_MDP", v1a_sql, "V1A — SELL_OUT OLD (GROUP BY)")
df_new = run_sf_query("PRD_MDP", v1b_sql, "V1B — SELL_OUT NEW (no GROUP BY)")

log_df(df_old, "OLD totals")
log_df(df_new, "NEW totals")

# COMMAND ----------

# MAGIC %md
# MAGIC ## V2 — SELL_IN: Brand-Level Rollup Check

# COMMAND ----------

v2_sql = """
SELECT
    CBU,
    MARCA,
    FECHA AS MES,
    SUM(VOLUMEN) AS TOTAL_VOL,
    SUM(VALOR)   AS TOTAL_VAL,
    COUNT(*)     AS ROW_COUNT,
    COUNT(DISTINCT SKU) AS DISTINCT_SKUS,
    SUM(CASE WHEN SKU IS NULL THEN 1 ELSE 0 END) AS NULL_SKUS
FROM (
    SELECT
        FAC.CBU,
        PRO.LV2_UMB_BRD_DSC AS MARCA,
        DATE_TRUNC('MONTH', TO_DATE(TO_VARCHAR(FAC.BIL_DAT), 'YYYYMMDD')) AS FECHA,
        FAC.MAT_IDT AS SKU,
        SUM(FAC.LITER) AS VOLUMEN,
        SUM(FAC.BIL_INV) AS VALOR
    FROM PRD_MEX.MEX_DSP_OTC.VW_FACT_RNV FAC
    LEFT JOIN PRD_MEX.MEX_DSP_OTC.V_D_ITEM PRO
        ON FAC.MAT_IDT = PRO.MAT_IDT
    WHERE FAC.CBU IN ('WATERS')
      AND FAC.BIL_DOC_TYP_COD NOT IN ('ZINT', 'ZPIO')
      AND DATE_TRUNC('MONTH', TO_DATE(TO_VARCHAR(FAC.BIL_DAT), 'YYYYMMDD')) >= '2025-01-01'
    GROUP BY
        FAC.CBU,
        PRO.LV2_UMB_BRD_DSC,
        DATE_TRUNC('MONTH', TO_DATE(TO_VARCHAR(FAC.BIL_DAT), 'YYYYMMDD')),
        FAC.MAT_IDT
) src
GROUP BY
    CBU,
    MARCA,
    FECHA
ORDER BY
    CBU,
    MARCA,
    FECHA
"""
log("V2: SELL_IN brand rollup + SKU null check")
df_si = run_sf_query("PRD_MEX", v2_sql, "V2 — SELL_IN brand rollup (WATERS)")
log_df(df_si, "SELL_IN brand rollup", n=50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## V3 — IBP: FECHA and MARCA Null Rates

# COMMAND ----------

v3_sql = """
SELECT
    COUNT(*)                                           AS TOTAL_ROWS,
    SUM(CASE WHEN FECHA IS NULL THEN 1 ELSE 0 END)    AS NULL_FECHA,
    SUM(CASE WHEN MARCA IS NULL THEN 1 ELSE 0 END)    AS NULL_MARCA,
    SUM(CASE WHEN VALOR IS NULL THEN 1 ELSE 0 END)    AS NULL_VALOR,
    ROUND(100.0 * SUM(CASE WHEN FECHA IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS PCT_NULL_FECHA,
    ROUND(100.0 * SUM(CASE WHEN MARCA IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS PCT_NULL_MARCA,
    MIN(FECHA) AS MIN_FECHA,
    MAX(FECHA) AS MAX_FECHA,
    COUNT(DISTINCT MARCA) AS DISTINCT_MARCAS
FROM PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP
"""

log("V3: IBP null rates for FECHA and MARCA")
df_ibp = run_sf_query("PRD_MDP", v3_sql, "V3 — IBP null rates")
log_df(df_ibp, "IBP null rates")

# COMMAND ----------

# MAGIC %md
# MAGIC ## V4 — WASTE: SKU + WASTE_KG Validation

# COMMAND ----------

v4_sql = """
SELECT
    COUNT(*)                                          AS TOTAL_ROWS,
    SUM(CASE WHEN SKU IS NULL THEN 1 ELSE 0 END)     AS NULL_SKU,
    SUM(CASE WHEN "Waste (KG)" IS NULL THEN 1 ELSE 0 END) AS NULL_WASTE_KG,
    SUM(CASE WHEN "Waste ($)" IS NULL THEN 1 ELSE 0 END)  AS NULL_WASTE_AMT,
    ROUND(100.0 * SUM(CASE WHEN SKU IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS PCT_NULL_SKU,
    COUNT(DISTINCT SKU)     AS DISTINCT_SKUS,
    MIN("Waste (KG)")       AS MIN_KG,
    MAX("Waste (KG)")       AS MAX_KG,
    SUM("Waste (KG)")       AS TOTAL_KG,
    SUM("Waste ($)")        AS TOTAL_AMT
FROM PRD_MDP.MDP_STG.VW_WASTE
"""

log("V4: WASTE SKU + WASTE_KG check")
df_waste = run_sf_query("PRD_MDP", v4_sql, "V4 — WASTE SKU + KG")
log_df(df_waste, "WASTE stats")

# COMMAND ----------

# MAGIC %md
# MAGIC ## V5 — Investment: Shared Columns in MKT_ON and MKT_OFF

# COMMAND ----------

v5a_sql = """
SELECT
    'MKT_ON' AS SOURCE,
    COUNT(*) AS TOTAL_ROWS,
    SUM(CASE WHEN MEDIO IS NULL THEN 1 ELSE 0 END) AS NULL_MEDIO,
    SUM(CASE WHEN SOPORTE_PLATAFORMA IS NULL THEN 1 ELSE 0 END) AS NULL_SOPORTE,
    SUM(CASE WHEN MARCA IS NULL THEN 1 ELSE 0 END) AS NULL_MARCA,
    SUM(CASE WHEN CAMPANA IS NULL THEN 1 ELSE 0 END) AS NULL_CAMPANA,
    SUM(CASE WHEN FECHA IS NULL THEN 1 ELSE 0 END) AS NULL_FECHA,
    SUM(CASE WHEN INVERSION_REAL IS NULL THEN 1 ELSE 0 END) AS NULL_INV,
    SUM(INVERSION_REAL) AS TOTAL_INV,
    COUNT(DISTINCT MEDIO) AS DIST_MEDIO,
    COUNT(DISTINCT SOPORTE_PLATAFORMA) AS DIST_SOPORTE,
    COUNT(DISTINCT MARCA) AS DIST_MARCA,
    COUNT(DISTINCT CAMPANA) AS DIST_CAMPANA
FROM PRD_MDP.MDP_DSP.VW_MKT_ECOMM
WHERE ANIO >= 2024
"""

v5b_sql = """
SELECT
    'MKT_OFF' AS SOURCE,
    COUNT(*) AS TOTAL_ROWS,
    SUM(CASE WHEN MEDIO IS NULL THEN 1 ELSE 0 END) AS NULL_MEDIO,
    SUM(CASE WHEN SOPORTE_PLATAFORMA IS NULL THEN 1 ELSE 0 END) AS NULL_SOPORTE,
    SUM(CASE WHEN MARCA IS NULL THEN 1 ELSE 0 END) AS NULL_MARCA,
    SUM(CASE WHEN CAMPANA IS NULL THEN 1 ELSE 0 END) AS NULL_CAMPANA,
    SUM(CASE WHEN FECHA IS NULL THEN 1 ELSE 0 END) AS NULL_FECHA,
    SUM(CASE WHEN INVERSION_REAL IS NULL THEN 1 ELSE 0 END) AS NULL_INV,
    SUM(INVERSION_REAL) AS TOTAL_INV,
    COUNT(DISTINCT MEDIO) AS DIST_MEDIO,
    COUNT(DISTINCT SOPORTE_PLATAFORMA) AS DIST_SOPORTE,
    COUNT(DISTINCT MARCA) AS DIST_MARCA,
    COUNT(DISTINCT CAMPANA) AS DIST_CAMPANA
FROM PRD_MDP.MDP_STG.FACT_MEDIA_OFF
WHERE ANIO >= 2024
"""

log("V5: Investment shared columns — MKT_ON")
df_on = run_sf_query("PRD_MDP", v5a_sql, "V5A — MKT_ON columns")
log_df(df_on, "MKT_ON columns")

log("V5: Investment shared columns — MKT_OFF")
df_off = run_sf_query("PRD_MDP", v5b_sql, "V5B — MKT_OFF columns")
log_df(df_off, "MKT_OFF columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## V6 — SELL_OUT Column Types

# COMMAND ----------

v6_sql = """
SELECT
    SYSTEM$TYPEOF(f.STORE)      AS STORE_TYPE,
    SYSTEM$TYPEOF(f.UPC)        AS UPC_TYPE,
    SYSTEM$TYPEOF(st.SUBCHAIN)  AS SUBCHAIN_TYPE,
    SYSTEM$TYPEOF(st.CHAIN)     AS CHAIN_TYPE,
    SYSTEM$TYPEOF(st.FORMAT)    AS FORMAT_TYPE,
    SYSTEM$TYPEOF(prod.BRAND)   AS BRAND_TYPE
FROM PRD_MDP.MDP_DSP.VW_FACT_SELL_OUT f
INNER JOIN PRD_MDP.MDP_DSP.VW_D_STORE_RM st
    ON f.STORE = st.INT_ID
   AND f.CBU_ID = st.CBU_ID
INNER JOIN PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM prod
    ON f.UPC = prod.INT_ID
   AND f.CBU_ID = prod.CBU_ID
LIMIT 1
"""

log("V6: SELL_OUT column types")
df_types = run_sf_query("PRD_MDP", v6_sql, "V6 — SELL_OUT types")
log_df(df_types, "SELL_OUT column types")

# COMMAND ----------

# MAGIC %md
# MAGIC ## V7 — Cross-Source MARCA Comparison

# COMMAND ----------

v7a_sql = """
SELECT DISTINCT
    TRIM(BRAND) AS MARCA
FROM PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM
WHERE BRAND IS NOT NULL
  AND TRIM(BRAND) <> ''
ORDER BY MARCA
"""

v7b_sql = """
SELECT DISTINCT
    TRIM(LV2_UMB_BRD_DSC) AS MARCA
FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM
WHERE LV2_UMB_BRD_DSC IS NOT NULL
  AND TRIM(LV2_UMB_BRD_DSC) <> ''
ORDER BY MARCA
"""

v7c_sql = """
SELECT DISTINCT
    TRIM(MARCA) AS MARCA
FROM PRD_MDP.MDP_DSP.VW_MKT_ECOMM
WHERE MARCA IS NOT NULL
  AND TRIM(MARCA) <> ''
ORDER BY MARCA
"""

v7d_sql = """
SELECT DISTINCT
    TRIM(MARCA) AS MARCA
FROM PRD_MDP.MDP_STG.FACT_MEDIA_OFF
WHERE MARCA IS NOT NULL
  AND TRIM(MARCA) <> ''
ORDER BY MARCA
"""

v7e_sql = """
SELECT DISTINCT
    TRIM(MARCA) AS MARCA
FROM PRD_MDP.MDP_STG.VW_WASTE
WHERE MARCA IS NOT NULL
  AND TRIM(MARCA) <> ''
ORDER BY MARCA
"""

v7f_sql = """
SELECT DISTINCT
    TRIM(MARCA) AS MARCA
FROM PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP
WHERE MARCA IS NOT NULL
  AND TRIM(MARCA) <> ''
ORDER BY MARCA
"""

log("V7: Cross-source MARCA comparison")

log("V7A — SELL_OUT brands (PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM.BRAND)")
df_m_so = run_sf_query("PRD_MDP", v7a_sql, "V7A — SELL_OUT MARCA")
log_df(df_m_so, "SELL_OUT MARCA")

log("V7B — SELL_IN brands (PRD_MEX.MEX_DSP_OTC.V_D_ITEM.LV2_UMB_BRD_DSC)")
df_m_si = run_sf_query("PRD_MEX", v7b_sql, "V7B — SELL_IN MARCA")
log_df(df_m_si, "SELL_IN MARCA")

log("V7C — MKT_ON brands (PRD_MDP.MDP_DSP.VW_MKT_ECOMM.MARCA)")
df_m_on = run_sf_query("PRD_MDP", v7c_sql, "V7C — MKT_ON MARCA")
log_df(df_m_on, "MKT_ON MARCA")

log("V7D — MKT_OFF brands (PRD_MDP.MDP_STG.FACT_MEDIA_OFF.MARCA)")
df_m_off = run_sf_query("PRD_MDP", v7d_sql, "V7D — MKT_OFF MARCA")
log_df(df_m_off, "MKT_OFF MARCA")

log("V7E — WASTE brands (PRD_MDP.MDP_STG.VW_WASTE.MARCA)")
df_m_wa = run_sf_query("PRD_MDP", v7e_sql, "V7E — WASTE MARCA")
log_df(df_m_wa, "WASTE MARCA")

log("V7F — IBP brands (PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP.MARCA)")
df_m_ibp = run_sf_query("PRD_MDP", v7f_sql, "V7F — IBP MARCA")
log_df(df_m_ibp, "IBP MARCA")

# COMMAND ----------

# MAGIC %md
# MAGIC ## V8 — SELL_OUT Row Count Impact

# COMMAND ----------

v8_sql = """
WITH fact_filtered AS (
    SELECT PER_ID, STORE, UPC, VOL_SELL_OUT, PCS_SELL_OUT, AMOUNT_SELL_OUT, VOL_INV, PCS_INV, CBU_ID
    FROM PRD_MDP.MDP_DSP.VW_FACT_SELL_OUT
    WHERE PER_ID >= 20250101
),
base AS (
    SELECT
        per."DAY" AS FECHA, st.CHAIN AS CADENA, st.FORMAT AS FORMATO_CADENA, prod.BRAND AS MARCA,
        f.STORE AS STORE_ID, TO_VARCHAR(f.UPC) AS UPC, st.SUBCHAIN AS SUBCHAIN,
        f.VOL_SELL_OUT, f.PCS_SELL_OUT, f.AMOUNT_SELL_OUT, f.VOL_INV, f.PCS_INV
    FROM fact_filtered f
    INNER JOIN PRD_MDP.MDP_DWH.V_D_PERIOD per ON f.PER_ID = per.PER_ID
    INNER JOIN PRD_MDP.MDP_DSP.VW_D_STORE_RM st ON f.STORE = st.INT_ID AND f.CBU_ID = st.CBU_ID
    INNER JOIN PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM prod ON TO_VARCHAR(f.UPC) = TO_VARCHAR(prod.INT_ID) AND f.CBU_ID = prod.CBU_ID
)
SELECT
    DATE_TRUNC('MONTH', FECHA) AS MES,
    COUNT(*) AS NEW_ROWS,
    COUNT(DISTINCT STORE_ID) AS DISTINCT_STORES,
    COUNT(DISTINCT UPC) AS DISTINCT_UPCS,
    COUNT(DISTINCT CADENA) AS DISTINCT_CADENAS,
    COUNT(DISTINCT MARCA) AS DISTINCT_MARCAS,
    COUNT(DISTINCT SUBCHAIN) AS DISTINCT_SUBCHAINS
FROM base
GROUP BY 1
ORDER BY 1
"""

log("V8: SELL_OUT row count impact at new grain")
df_impact = run_sf_query("PRD_MDP", v8_sql, "V8 — SELL_OUT row count + cardinality")
log_df(df_impact, "SELL_OUT row count + cardinality", n=50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## V9 — Nielsen: Distinct Brand Values (C.4 — BLOCKED dependency)
# MAGIC Run this to unblock C.5: adding Nielsen brands to the crosswalk.

# COMMAND ----------

# V9A: EDP Nielsen — ITM_UNIF_BRND (from INP_56985)
v9a_sql = """
SELECT DISTINCT TRIM(INP_56985) AS MARCA, 'EDP_MARKET' AS SOURCE
FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_PROD_DIM
WHERE INP_56985 IS NOT NULL AND TRIM(INP_56985) <> ''
ORDER BY MARCA
"""

# V9B: Water Retail — ITM_UNIF_BRAND (from ITM_UNIF_BRND_DAN)
v9b_sql = """
SELECT DISTINCT TRIM(A.ITM_UNIF_BRND_DAN) AS MARCA, 'WATER_RETAIL' AS SOURCE
FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_D_MKT_WATER_RETAIL_PROD_DIM A
WHERE A.ITM_UNIF_BRND_DAN IS NOT NULL AND TRIM(A.ITM_UNIF_BRND_DAN) <> ''
ORDER BY MARCA
"""

# V9C: Water Scantrack — ITM_UNIF_BRAND (from ITM_UNIF_BRND_DAN)
v9c_sql = """
SELECT DISTINCT TRIM(A.ITM_UNIF_BRND_DAN) AS MARCA, 'WATER_SCANTRACK' AS SOURCE
FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_D_MKT_WATER_SCANTRACK_PROD_DIM A
WHERE A.ITM_UNIF_BRND_DAN IS NOT NULL AND TRIM(A.ITM_UNIF_BRND_DAN) <> ''
ORDER BY MARCA
"""

# V9D: PB Scantrack — ITM_UNIF_BRAND (from INP_56985)
v9d_sql = """
SELECT DISTINCT TRIM(B.INP_56985) AS MARCA, 'PB_SCANTRACK' AS SOURCE
FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_D_MKT_PB_SCANTRACK_PROD_DIM B
WHERE B.INP_56985 IS NOT NULL AND TRIM(B.INP_56985) <> ''
ORDER BY MARCA
"""

log("=" * 70)
log("V9: Nielsen distinct brand values (unblocks C.5)")
log("=" * 70)
log("NOTE: If any of these fail with 'object does not exist', the table name")
log("  needs to be confirmed via extract_column_types. Report the error.")

try:
    log("V9A — EDP Nielsen brands (INP_56985)")
    df_n_edp = run_sf_query("PRD_MEX", v9a_sql, "V9A — EDP MARCA")
    log_df(df_n_edp, "EDP Nielsen brands", n=300)
except Exception as e:
    log(f"  ❌ V9A FAILED: {e}")

try:
    log("V9B — Water Retail brands (ITM_UNIF_BRND_DAN)")
    df_n_wr = run_sf_query("PRD_MEX", v9b_sql, "V9B — Water Retail MARCA")
    log_df(df_n_wr, "Water Retail brands", n=300)
except Exception as e:
    log(f"  ❌ V9B FAILED: {e}")

try:
    log("V9C — Water Scantrack brands (ITM_UNIF_BRND_DAN)")
    df_n_ws = run_sf_query("PRD_MEX", v9c_sql, "V9C — Water Scantrack MARCA")
    log_df(df_n_ws, "Water Scantrack brands", n=300)
except Exception as e:
    log(f"  ❌ V9C FAILED: {e}")

try:
    log("V9D — PB Scantrack brands (INP_56985)")
    df_n_pb = run_sf_query("PRD_MEX", v9d_sql, "V9D — PB Scantrack MARCA")
    log_df(df_n_pb, "PB Scantrack brands", n=300)
except Exception as e:
    log(f"  ❌ V9D FAILED: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## V10 — MARCA_STD Spot Check (verify crosswalk mapping)

# COMMAND ----------

v10a_sql = """
SELECT
    MARCA,
    CASE
        WHEN TRIM(UPPER(MARCA)) IN ('ACTIVIA') THEN 'ACTIVIA'
        WHEN TRIM(UPPER(MARCA)) IN ('AGUAS FRESCAS', 'BFT AGUAS FRESCAS', 'BONAFONT_AGUASFRESCAS', 'BONAFONT AGUA FRESCAS') THEN 'AGUAS FRESCAS'
        WHEN TRIM(UPPER(MARCA)) IN ('BENEGASTRO', 'BENEG') THEN 'BENEGASTRO'
        WHEN TRIM(UPPER(MARCA)) IN ('BONAFONT', 'BONAFONT_NATURAL', 'AZUL BONAFONT') THEN 'BONAFONT'
        WHEN TRIM(UPPER(MARCA)) IN ('BONAFONT KIDS', 'BONAFONT_KIDS', 'BONAFONT KIDS NATURAL') THEN 'BONAFONT KIDS'
        WHEN TRIM(UPPER(MARCA)) IN ('TE BONAFONT', 'TÉ BONAFONT', 'BONAFONT TE', 'BONAFONT_TE', 'BONAFONT TÉ') THEN 'BONAFONT TE'
        WHEN TRIM(UPPER(MARCA)) IN ('DANMIX', 'DAN MIX', 'DANMMIX') THEN 'DANMIX'
        WHEN TRIM(UPPER(MARCA)) IN ('DANONE', 'DANONE YOGHURT', 'DANONE CREME', 'DANONE FREE', 'DANONE GRIEGO', 'DANONE FS', 'DAIRY') THEN 'DANONE'
        WHEN TRIM(UPPER(MARCA)) IN ('DANONINO', 'DANONINOLIQUIDO') THEN 'DANONINO'
        WHEN TRIM(UPPER(MARCA)) IN ('DANUP', 'DAN UP', 'DAN''UP') THEN 'DANUP'
        WHEN TRIM(UPPER(MARCA)) IN ('DANY', 'DANY DANETTE') THEN 'DANY'
        WHEN TRIM(UPPER(MARCA)) IN ('HERSHEYS', 'HERSHEY''S', 'DANONE HERSHEYS') THEN 'HERSHEYS'
        WHEN TRIM(UPPER(MARCA)) IN ('LEVITE', 'BONAFONT_LEVITE', 'BONAFONT LEVITE') THEN 'LEVITE'
        WHEN TRIM(UPPER(MARCA)) IN ('OCEAN SPRAY', 'OCEAN') THEN 'OCEAN SPRAY'
        WHEN TRIM(UPPER(MARCA)) IN ('OIKOS', 'OIKOS UHT') THEN 'OIKOS'
        WHEN TRIM(UPPER(MARCA)) IN ('SILK', 'SILK ORIG 946ML', 'SILKCHOCO190ML') THEN 'SILK'
        WHEN TRIM(UPPER(MARCA)) IN ('STOK COLD BREW', 'STOK') THEN 'STOK'
        WHEN TRIM(UPPER(MARCA)) IN ('YOPRO', 'YO PRO') THEN 'YOPRO'
        WHEN TRIM(UPPER(MARCA)) IN ('COCA COLA', 'COCA-COLA', 'COCACOLA', 'THE COCA COLA EXPORT', 'COCA COLA FEMSA') THEN 'COCA COLA'
        WHEN TRIM(UPPER(MARCA)) IN ('PEPSI', 'PEPSI COLA MEXICANA', 'PEPSICOLA MEXICANA') THEN 'PEPSI'
        WHEN TRIM(UPPER(MARCA)) IN ('LALA', 'GPO INDUSTRIAL LALA', 'LALA_BIO4', 'LALA_CREMA', 'LALA_GRIEGO', 'LALA_LECHE', 'LALA_YOGURT') THEN 'LALA'
        WHEN TRIM(UPPER(MARCA)) IN ('LONCHERA', 'PALAS', 'TABLAS', 'TAZAS', 'VASOS', 'TOALLAS', 'TUPPER', 'UTENSILIOS', 'CERAMICA', 'VAJILLA') THEN '_MERCHANDISE'
        WHEN TRIM(UPPER(MARCA)) IN ('0', 'MULTI', 'DAIRY') THEN '_UNKNOWN'
        WHEN TRIM(UPPER(MARCA)) IN ('MULTIBRAND', 'MULTIBRAND DAIRY', 'MULTIBRAND DANONE', 'MULTIMARCA', 'DNP') THEN '_MULTIBRAND'
        ELSE TRIM(UPPER(MARCA))
    END AS MARCA_STD,
    COUNT(*) AS ROW_COUNT
FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM
WHERE LV2_UMB_BRD_DSC IS NOT NULL
GROUP BY MARCA, MARCA_STD
ORDER BY MARCA_STD, MARCA
"""

v10b_sql = """
SELECT
    t.MARCA,
    CASE
        WHEN TRIM(UPPER(t.MARCA)) IN ('ACTIVIA') THEN 'ACTIVIA'
        WHEN TRIM(UPPER(t.MARCA)) IN ('BENEGASTRO', 'BENEG') THEN 'BENEGASTRO'
        WHEN TRIM(UPPER(t.MARCA)) IN ('BONAFONT', 'BONAFONT_NATURAL', 'AZUL BONAFONT') THEN 'BONAFONT'
        WHEN TRIM(UPPER(t.MARCA)) IN ('TE BONAFONT', 'TÉ BONAFONT', 'BONAFONT TE', 'BONAFONT_TE') THEN 'BONAFONT TE'
        WHEN TRIM(UPPER(t.MARCA)) IN ('DANMIX', 'DAN MIX') THEN 'DANMIX'
        WHEN TRIM(UPPER(t.MARCA)) IN ('DANONE', 'DANONE FREE', 'DANONE GRIEGO') THEN 'DANONE'
        WHEN TRIM(UPPER(t.MARCA)) IN ('DANUP', 'DAN UP') THEN 'DANUP'
        WHEN TRIM(UPPER(t.MARCA)) IN ('HERSHEYS', 'HERSHEY''S') THEN 'HERSHEYS'
        WHEN TRIM(UPPER(t.MARCA)) IN ('LEVITE', 'BONAFONT_LEVITE') THEN 'LEVITE'
        WHEN TRIM(UPPER(t.MARCA)) IN ('MULTIBRAND', 'MULTIBRAND DAIRY', 'MULTIBRAND DANONE', 'MULTIMARCA', 'DNP') THEN '_MULTIBRAND'
        ELSE TRIM(UPPER(t.MARCA))
    END AS MARCA_STD,
    COUNT(*) AS ROW_COUNT
FROM PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP t
WHERE t.MARCA IS NOT NULL
GROUP BY t.MARCA, MARCA_STD
ORDER BY MARCA_STD, t.MARCA
"""

log("V10: MARCA_STD spot check")
log("V10A — SELL_IN (LV2_UMB_BRD_DSC → MARCA_STD)")
df_v10a = run_sf_query("PRD_MEX", v10a_sql, "V10A — SELL_IN MARCA_STD spot check")
log_df(df_v10a, "SELL_IN MARCA_STD mapping", n=200)

log("V10B — IBP (MARCA → MARCA_STD)")
df_v10b = run_sf_query("PRD_MDP", v10b_sql, "V10B — IBP MARCA_STD spot check")
log_df(df_v10b, "IBP MARCA_STD mapping", n=100)

# COMMAND ----------

# MAGIC %md
# MAGIC ## V11 — Cross-Source Join Test (SELL_OUT × IBP on MARCA_STD + FECHA)

# COMMAND ----------

v11_sql = """
WITH sell_out_std AS (
    SELECT
        DATE_TRUNC('MONTH', per."DAY") AS FECHA,
        CASE
            WHEN TRIM(UPPER(prod.BRAND)) IN ('ACTIVIA') THEN 'ACTIVIA'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('BONAFONT', 'BONAFONT_NATURAL', 'AZUL BONAFONT') THEN 'BONAFONT'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('TE BONAFONT', 'TÉ BONAFONT', 'BONAFONT TE', 'BONAFONT_TE') THEN 'BONAFONT TE'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('DANMIX', 'DAN MIX', 'DANMMIX') THEN 'DANMIX'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('DANONE', 'DANONE YOGHURT', 'DANONE CREME', 'DANONE FREE', 'DANONE GRIEGO', 'DANONE FS') THEN 'DANONE'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('DANONINO', 'DANONINOLIQUIDO') THEN 'DANONINO'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('DANUP', 'DAN UP', 'DAN''UP') THEN 'DANUP'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('DANY', 'DANY DANETTE') THEN 'DANY'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('EVIAN') THEN 'EVIAN'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('HERSHEYS', 'HERSHEY''S', 'DANONE HERSHEYS') THEN 'HERSHEYS'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('LEVITE', 'BONAFONT_LEVITE', 'BONAFONT LEVITE') THEN 'LEVITE'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('OIKOS', 'OIKOS UHT') THEN 'OIKOS'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('SILK', 'SILK ORIG 946ML', 'SILKCHOCO190ML') THEN 'SILK'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('VITALINEA') THEN 'VITALINEA'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('YOPRO', 'YO PRO') THEN 'YOPRO'
            ELSE TRIM(UPPER(prod.BRAND))
        END AS MARCA_STD,
        SUM(f.VOL_SELL_OUT)    AS SO_VOL,
        SUM(f.AMOUNT_SELL_OUT) AS SO_AMT
    FROM PRD_MDP.MDP_DSP.VW_FACT_SELL_OUT f
    INNER JOIN PRD_MDP.MDP_DWH.V_D_PERIOD per ON f.PER_ID = per.PER_ID
    INNER JOIN PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM prod
        ON f.UPC = prod.INT_ID AND f.CBU_ID = prod.CBU_ID
    WHERE f.PER_ID >= 20250101
    GROUP BY 1, 2
),
ibp_std AS (
    SELECT
        DATE_TRUNC('MONTH', t.FECHA) AS FECHA,
        CASE
            WHEN TRIM(UPPER(t.MARCA)) IN ('ACTIVIA') THEN 'ACTIVIA'
            WHEN TRIM(UPPER(t.MARCA)) IN ('BONAFONT') THEN 'BONAFONT'
            WHEN TRIM(UPPER(t.MARCA)) IN ('BONAFONT TÉ') THEN 'BONAFONT TE'
            WHEN TRIM(UPPER(t.MARCA)) IN ('DAN MIX') THEN 'DANMIX'
            WHEN TRIM(UPPER(t.MARCA)) IN ('DANONE', 'DANONE FREE', 'DANONE GRIEGO') THEN 'DANONE'
            WHEN TRIM(UPPER(t.MARCA)) IN ('DANONINO') THEN 'DANONINO'
            WHEN TRIM(UPPER(t.MARCA)) IN ('DAN UP') THEN 'DANUP'
            WHEN TRIM(UPPER(t.MARCA)) IN ('DANY') THEN 'DANY'
            WHEN TRIM(UPPER(t.MARCA)) IN ('EVIAN') THEN 'EVIAN'
            WHEN TRIM(UPPER(t.MARCA)) IN ('HERSHEY''S') THEN 'HERSHEYS'
            WHEN TRIM(UPPER(t.MARCA)) IN ('LEVITE') THEN 'LEVITE'
            WHEN TRIM(UPPER(t.MARCA)) IN ('OIKOS') THEN 'OIKOS'
            WHEN TRIM(UPPER(t.MARCA)) IN ('SILK') THEN 'SILK'
            WHEN TRIM(UPPER(t.MARCA)) IN ('VITALINEA') THEN 'VITALINEA'
            ELSE TRIM(UPPER(t.MARCA))
        END AS MARCA_STD,
        SUM(t.VALOR) AS IBP_AMT
    FROM PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP t
    WHERE t.NOMBRE_ETIQUETA = 'REAL'
      AND t.FECHA >= '2025-01-01'
    GROUP BY 1, 2
)
SELECT
    s.FECHA,
    s.MARCA_STD,
    ROUND(s.SO_VOL, 2)   AS SO_VOL,
    ROUND(s.SO_AMT, 2)   AS SO_AMT,
    ROUND(i.IBP_AMT, 2)  AS IBP_AMT,
    CASE WHEN i.MARCA_STD IS NULL THEN 'NO_IBP_MATCH' ELSE 'MATCHED' END AS JOIN_STATUS
FROM sell_out_std s
LEFT JOIN ibp_std i ON s.FECHA = i.FECHA AND s.MARCA_STD = i.MARCA_STD
WHERE s.MARCA_STD NOT IN ('_MERCHANDISE', '_UNKNOWN', '_MULTIBRAND', 'INACTIVO', 'NA', 'OTROS')
ORDER BY s.FECHA, s.MARCA_STD
"""

log("V11: Cross-source join test — SELL_OUT × IBP on MARCA_STD + FECHA")
df_v11 = run_sf_query("PRD_MDP", v11_sql, "V11 — cross-source MARCA_STD join")
log_df(df_v11, "SELL_OUT × IBP join result", n=200)

# Summary of join status
v11_summary_sql = """
SELECT
    JOIN_STATUS,
    COUNT(DISTINCT MARCA_STD) AS DISTINCT_BRANDS,
    COUNT(*) AS PERIOD_BRAND_ROWS
FROM (
""" + v11_sql.replace("ORDER BY s.FECHA, s.MARCA_STD", "") + """
)
GROUP BY JOIN_STATUS
"""
# Simpler inline summary
log("V11 — join status summary (run after V11 main if needed):")
log("  → Count rows where JOIN_STATUS = 'NO_IBP_MATCH' — those are SELL_OUT brands missing from IBP.")
log("  → Count rows where JOIN_STATUS = 'MATCHED' — those are joinable cross-source.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Phase C Log

# COMMAND ----------

log("=" * 70)
log("PHASE C VALIDATION COMPLETE")
log("=" * 70)

# Save to a Phase C specific file
import os
try:
    log_dir = os.path.dirname(LOG_PATH)
    phase_c_path = os.path.join(log_dir, "validation_results_phase_c.txt")
    os.makedirs(log_dir, exist_ok=True)
    with open(phase_c_path, "w") as f:
        f.write("\n".join(LOG_LINES))
    log(f"Phase C log saved to: {phase_c_path}")
    print(f"✓ Log saved to: {phase_c_path}")
except Exception as e:
    fallback = "/tmp/validation_results_phase_c.txt"
    with open(fallback, "w") as f:
        f.write("\n".join(LOG_LINES))
    print(f"Saved to fallback: {fallback}")

print("\n" + "=" * 70)
print("PHASE C VALIDATION COMPLETE")
print("=" * 70)
print("\n".join(LOG_LINES[-50:]))  # print last 50 lines as summary
