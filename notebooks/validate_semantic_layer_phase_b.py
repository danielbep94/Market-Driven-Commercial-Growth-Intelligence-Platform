q# Databricks notebook source
# MAGIC %md
# MAGIC # Semantic Layer — Snowflake Validation Queries
# MAGIC **Purpose**: Run all validation queries against Snowflake and log results.
# MAGIC
# MAGIC Run each cell in order. Results are printed and saved to `/tmp/semantic_validation_log.txt`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — Snowflake Connections

# COMMAND ----------

# ── PRD_MDP Connection (SELL_OUT, WASTE, IBP, MKT_ON, MKT_OFF) ──────────

sf_opts_mdp = {
    "sfURL":       "danonenam.east-us-2.azure.snowflakecomputing.com",
    "sfDatabase":  "PRD_MDP",
    "sfWarehouse": "PRD_MDP_ANL_WH",  # adjust if different
}

# ── PRD_MEX Connection (SELL_IN, Nielsen) ────────────────────────────────

sf_opts_mex = {
    "sfURL":       "danonenam.east-us-2.azure.snowflakecomputing.com",
    "sfUser":      "PRD_OSM_DPH_READER",
    "sfPassword":  "73.bBZmne7Aq",
    "sfDatabase":  "PRD_MEX",
    "sfSchema":    "MEX_DSP_DPH_MKT",
    "sfWarehouse": "PRD_MEX_ANL_WH",
    "sfRole":      "PRD_MEX_READER",
}

# COMMAND ----------

import datetime

LOG_LINES = []

def log(msg):
    """Print and buffer a log line."""
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    LOG_LINES.append(line)

def run_sf_query(opts, query, label="query"):
    """Run a Snowflake query via Spark and return a Spark DataFrame."""
    log(f"Running: {label}")
    df = (spark.read
          .format("net.snowflake.spark.snowflake")
          .options(**opts)
          .option("query", query)
          .load())
    log(f"  → {df.count()} rows returned")
    return df

def save_log():
    """Write accumulated log to file."""
    path = "/tmp/semantic_validation_log.txt"
    with open(path, "w") as f:
        f.write("\n".join(LOG_LINES))
    log(f"Log saved to {path}")

log("=" * 70)
log("SEMANTIC LAYER VALIDATION — Phase B")
log("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## V1 — SELL_OUT: Monthly Totals (Old vs New Grain)

# COMMAND ----------

# V1A: OLD query (with GROUP BY)
v1a_sql = """
WITH fact_filtered AS (
    SELECT PER_ID, STORE, UPC, VOL_SELL_OUT, PCS_SELL_OUT, AMOUNT_SELL_OUT, VOL_INV, PCS_INV, CBU_ID
    FROM PRD_MDP.MDP_DSP.VW_FACT_SELL_OUT
    WHERE PER_ID >= 20250101
),
base AS (
    SELECT
        per."DAY"  AS FECHA,
        st.CHAIN   AS CADENA,
        st.FORMAT  AS FORMATO_CADENA,
        prod.BRAND AS MARCA,
        f.VOL_SELL_OUT, f.PCS_SELL_OUT, f.AMOUNT_SELL_OUT, f.VOL_INV, f.PCS_INV
    FROM fact_filtered f
    INNER JOIN PRD_MDP.MDP_DWH.V_D_PERIOD per ON f.PER_ID = per.PER_ID
    INNER JOIN PRD_MDP.MDP_DSP.VW_D_STORE_RM st ON f.STORE = st.INT_ID AND f.CBU_ID = st.CBU_ID
    INNER JOIN PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM prod ON TO_VARCHAR(f.UPC) = TO_VARCHAR(prod.INT_ID) AND f.CBU_ID = prod.CBU_ID
),
old_agg AS (
    SELECT FECHA, CADENA, FORMATO_CADENA, MARCA,
           SUM(VOL_SELL_OUT) AS VOL_SELL_OUT, SUM(PCS_SELL_OUT) AS PCS_SELL_OUT,
           SUM(AMOUNT_SELL_OUT) AS AMOUNT_SELL_OUT, SUM(VOL_INV) AS VOL_INV, SUM(PCS_INV) AS PCS_INV
    FROM base GROUP BY FECHA, CADENA, FORMATO_CADENA, MARCA
)
SELECT
    DATE_TRUNC('MONTH', FECHA) AS MES,
    SUM(VOL_SELL_OUT)    AS VOL, SUM(PCS_SELL_OUT) AS PCS,
    SUM(AMOUNT_SELL_OUT) AS AMT, SUM(VOL_INV) AS V_INV, SUM(PCS_INV) AS P_INV,
    COUNT(*) AS ROWS
FROM old_agg GROUP BY 1 ORDER BY 1
"""

# V1B: NEW query (without GROUP BY)
v1b_sql = """
WITH fact_filtered AS (
    SELECT PER_ID, STORE, UPC, VOL_SELL_OUT, PCS_SELL_OUT, AMOUNT_SELL_OUT, VOL_INV, PCS_INV, CBU_ID
    FROM PRD_MDP.MDP_DSP.VW_FACT_SELL_OUT
    WHERE PER_ID >= 20250101
),
base AS (
    SELECT
        per."DAY"  AS FECHA, st.CHAIN AS CADENA, st.FORMAT AS FORMATO_CADENA, prod.BRAND AS MARCA,
        f.STORE AS STORE_ID, TO_VARCHAR(f.UPC) AS UPC, st.SUBCHAIN AS SUBCHAIN,
        f.VOL_SELL_OUT, f.PCS_SELL_OUT, f.AMOUNT_SELL_OUT, f.VOL_INV, f.PCS_INV
    FROM fact_filtered f
    INNER JOIN PRD_MDP.MDP_DWH.V_D_PERIOD per ON f.PER_ID = per.PER_ID
    INNER JOIN PRD_MDP.MDP_DSP.VW_D_STORE_RM st ON f.STORE = st.INT_ID AND f.CBU_ID = st.CBU_ID
    INNER JOIN PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM prod ON TO_VARCHAR(f.UPC) = TO_VARCHAR(prod.INT_ID) AND f.CBU_ID = prod.CBU_ID
)
SELECT
    DATE_TRUNC('MONTH', FECHA) AS MES,
    SUM(VOL_SELL_OUT) AS VOL, SUM(PCS_SELL_OUT) AS PCS,
    SUM(AMOUNT_SELL_OUT) AS AMT, SUM(VOL_INV) AS V_INV, SUM(PCS_INV) AS P_INV,
    COUNT(*) AS ROWS
FROM base GROUP BY 1 ORDER BY 1
"""

log("V1: SELL_OUT totals comparison")
df_old = run_sf_query(sf_opts_mdp, v1a_sql, "V1A — SELL_OUT OLD (GROUP BY)")
df_new = run_sf_query(sf_opts_mdp, v1b_sql, "V1B — SELL_OUT NEW (no GROUP BY)")

log("  OLD totals:")
df_old.show(truncate=False)
log("  NEW totals:")
df_new.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## V2 — SELL_IN: Brand-Level Rollup Check

# COMMAND ----------

v2_sql = """
SELECT
    CBU,
    MARCA,
    DATE_TRUNC('MONTH', FECHA) AS MES,
    SUM(VOLUMEN) AS TOTAL_VOL,
    SUM(VALOR)   AS TOTAL_VAL,
    COUNT(*)     AS ROWS,
    COUNT(DISTINCT SKU) AS DISTINCT_SKUS,
    SUM(CASE WHEN SKU IS NULL THEN 1 ELSE 0 END) AS NULL_SKUS
FROM (
    -- ============================================================
    -- Full SELL_IN query (both UNION legs with SKU)
    -- Paste your full SELL_IN query from SELL_IN_DICT.txt here
    -- or use the CTE version below which runs the raw tables
    -- ============================================================

    -- Simplified check: just count SKUs and totals from the raw fact
    SELECT
        FAC.CBU,
        PRO.LV2_UMB_BRD_DSC AS MARCA,
        DATE_TRUNC('MONTH', TO_DATE(TO_VARCHAR(FAC.BIL_DAT), 'YYYYMMDD')) AS FECHA,
        FAC.MAT_IDT AS SKU,
        SUM(FAC.LITER) AS VOLUMEN,
        SUM(FAC.BIL_INV) AS VALOR
    FROM PRD_MEX.MEX_DSP_OTC.VW_FACT_RNV FAC
    LEFT JOIN PRD_MEX.MEX_DSP_OTC.V_D_ITEM PRO ON FAC.MAT_IDT = PRO.MAT_IDT
    WHERE FAC.CBU IN ('WATERS')
      AND FAC.BIL_DOC_TYP_COD NOT IN ('ZINT', 'ZPIO')
      AND DATE_TRUNC('MONTH', TO_DATE(TO_VARCHAR(FAC.BIL_DAT), 'YYYYMMDD')) >= '2025-01-01'
    GROUP BY FAC.CBU, PRO.LV2_UMB_BRD_DSC, DATE_TRUNC('MONTH', TO_DATE(TO_VARCHAR(FAC.BIL_DAT), 'YYYYMMDD')), FAC.MAT_IDT
)
GROUP BY CBU, MARCA, MES
ORDER BY CBU, MARCA, MES
"""

log("V2: SELL_IN brand rollup + SKU null check")
df_si = run_sf_query(sf_opts_mex, v2_sql, "V2 — SELL_IN brand rollup (WATERS)")
df_si.show(50, truncate=False)

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
df_ibp = run_sf_query(sf_opts_mdp, v3_sql, "V3 — IBP null rates")
df_ibp.show(truncate=False)

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
df_waste = run_sf_query(sf_opts_mdp, v4_sql, "V4 — WASTE SKU + KG")
df_waste.show(truncate=False)

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
df_on = run_sf_query(sf_opts_mdp, v5a_sql, "V5A — MKT_ON columns")
df_on.show(truncate=False)

log("V5: Investment shared columns — MKT_OFF")
df_off = run_sf_query(sf_opts_mdp, v5b_sql, "V5B — MKT_OFF columns")
df_off.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## V6 — SELL_OUT Column Types

# COMMAND ----------

v6_sql = """
SELECT
    TYPEOF(f.STORE)           AS STORE_TYPE,
    TYPEOF(TO_VARCHAR(f.UPC)) AS UPC_TYPE,
    TYPEOF(st.SUBCHAIN)       AS SUBCHAIN_TYPE,
    TYPEOF(st.CHAIN)          AS CHAIN_TYPE,
    TYPEOF(st.FORMAT)         AS FORMAT_TYPE,
    TYPEOF(prod.BRAND)        AS BRAND_TYPE
FROM PRD_MDP.MDP_DSP.VW_FACT_SELL_OUT f
INNER JOIN PRD_MDP.MDP_DSP.VW_D_STORE_RM st ON f.STORE = st.INT_ID AND f.CBU_ID = st.CBU_ID
INNER JOIN PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM prod ON TO_VARCHAR(f.UPC) = TO_VARCHAR(prod.INT_ID) AND f.CBU_ID = prod.CBU_ID
LIMIT 1
"""

log("V6: SELL_OUT column types")
df_types = run_sf_query(sf_opts_mdp, v6_sql, "V6 — SELL_OUT types")
df_types.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## V7 — Cross-Source MARCA Comparison

# COMMAND ----------

v7a_sql = """
SELECT DISTINCT MARCA FROM PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM WHERE MARCA IS NOT NULL ORDER BY MARCA
"""
v7b_sql = """
SELECT DISTINCT LV2_UMB_BRD_DSC AS MARCA FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM WHERE LV2_UMB_BRD_DSC IS NOT NULL ORDER BY MARCA
"""
v7c_sql = """
SELECT DISTINCT MARCA FROM PRD_MDP.MDP_DSP.VW_MKT_ECOMM WHERE MARCA IS NOT NULL ORDER BY MARCA
"""
v7d_sql = """
SELECT DISTINCT MARCA FROM PRD_MDP.MDP_STG.FACT_MEDIA_OFF WHERE MARCA IS NOT NULL ORDER BY MARCA
"""
v7e_sql = """
SELECT DISTINCT MARCA FROM PRD_MDP.MDP_STG.VW_WASTE WHERE MARCA IS NOT NULL ORDER BY MARCA
"""
v7f_sql = """
SELECT DISTINCT MARCA FROM PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP WHERE MARCA IS NOT NULL ORDER BY MARCA
"""

log("V7: Cross-source MARCA comparison")
log("V7A — SELL_OUT brands (PRD_MDP.VW_D_PRODUCT_RM.BRAND)")
df_m_so = run_sf_query(sf_opts_mdp, v7a_sql, "V7A — SELL_OUT MARCA")
df_m_so.show(200, truncate=False)

log("V7B — SELL_IN brands (PRD_MEX.V_D_ITEM.LV2_UMB_BRD_DSC)")
df_m_si = run_sf_query(sf_opts_mex, v7b_sql, "V7B — SELL_IN MARCA")
df_m_si.show(200, truncate=False)

log("V7C — MKT_ON brands (VW_MKT_ECOMM.MARCA)")
df_m_on = run_sf_query(sf_opts_mdp, v7c_sql, "V7C — MKT_ON MARCA")
df_m_on.show(200, truncate=False)

log("V7D — MKT_OFF brands (FACT_MEDIA_OFF.MARCA)")
df_m_off = run_sf_query(sf_opts_mdp, v7d_sql, "V7D — MKT_OFF MARCA")
df_m_off.show(200, truncate=False)

log("V7E — WASTE brands (VW_WASTE.MARCA)")
df_m_wa = run_sf_query(sf_opts_mdp, v7e_sql, "V7E — WASTE MARCA")
df_m_wa.show(200, truncate=False)

log("V7F — IBP brands (VW_FACT_DANONE_IBP.MARCA)")
df_m_ibp = run_sf_query(sf_opts_mdp, v7f_sql, "V7F — IBP MARCA")
df_m_ibp.show(200, truncate=False)

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
df_impact = run_sf_query(sf_opts_mdp, v8_sql, "V8 — SELL_OUT row count + cardinality")
df_impact.show(50, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Log

# COMMAND ----------

log("=" * 70)
log("VALIDATION COMPLETE")
log("=" * 70)
save_log()

# Print summary
print("\n" + "=" * 70)
print("FULL LOG:")
print("=" * 70)
print("\n".join(LOG_LINES))
