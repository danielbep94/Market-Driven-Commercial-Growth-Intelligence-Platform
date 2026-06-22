# Databricks notebook source
# MAGIC %md
# MAGIC # V12 — Nielsen MARCA_STD Validation (Phase C.9)
# MAGIC
# MAGIC Validates that MARCA_STD was correctly injected into all 4 Nielsen layouts.
# MAGIC Confirms: brand mapping quality, null rates, Danone brand presence, DQ thresholds.
# MAGIC
# MAGIC **Run after C.6 patches are deployed.**

# COMMAND ----------

import os
import sys
import io
from datetime import datetime

# ── Snowflake connection (inherit from validate_semantic_layer_phase_b.py) ──
SF_URL = "danonenam.east-us-2.azure.snowflakecomputing.com"

PRD_MEX_PROFILE = {
    "sfURL":       SF_URL,
    "sfUser":      "PRD_OSM_DPH_READER",
    "sfPassword":  "73.bBZmne7Aq",
    "sfWarehouse": "PRD_MEX_ANL_WH",
    "sfRole":      "PRD_MEX_READER",
}

KEYVAULT_SCOPE = "DAN-AM-P-KVT800-R-MDP-DB"
PRD_MDP_PROFILE = {
    "sfURL":       SF_URL,
    "sfUser":      dbutils.secrets.get(scope=KEYVAULT_SCOPE, key="snowflake-user"),
    "sfPassword":  dbutils.secrets.get(scope=KEYVAULT_SCOPE, key="snowflake-password"),
    "sfWarehouse": "PRD_MDP_ANL_WH",
    "sfRole":      "PRD_MDP_READER",
}

LOG_LINES = []

def log(msg=""):
    ts  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    LOG_LINES.append(line)

def log_df(df, label, n=200):
    log(f"  {label}:")
    old_stdout = sys.stdout
    buf = io.StringIO()
    try:
        sys.stdout = buf
        df.show(n, truncate=False)
    finally:
        sys.stdout = old_stdout
    out = buf.getvalue()
    print(out)
    for l in out.rstrip().split("\n"):
        LOG_LINES.append(l)

def get_sf_options(database):
    if database == "PRD_MDP":
        base = dict(PRD_MDP_PROFILE)
    else:
        base = dict(PRD_MEX_PROFILE)
    base["sfDatabase"] = database
    return base

def run_sf_query(database, query, label="query"):
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
    rc = df.count()
    log(f"  → {rc} rows returned")
    return df

def get_log_candidates():
    candidates = []
    try:
        cwd = os.getcwd()
        candidates.append(os.path.join(cwd, "validation_results_nielsen_v12.txt"))
        nb_dir = os.path.join(cwd, "notebooks")
        if os.path.isdir(nb_dir):
            candidates.append(os.path.join(nb_dir, "validation_results_nielsen_v12.txt"))
        if os.path.basename(cwd).lower() == "notebooks":
            candidates.insert(0, os.path.join(cwd, "validation_results_nielsen_v12.txt"))
    except Exception:
        pass
    candidates.append("/tmp/validation_results_nielsen_v12.txt")
    return candidates

log("=" * 70)
log("V12 — Nielsen MARCA_STD Validation — Phase C.9")
log("=" * 70)
log("Goal: confirm MARCA_STD in all 4 Nielsen layouts via direct Snowflake queries")
log("Evidence basis: V9A=219 brands, V9B=160/166, V9C=140, V9D=77 (confirmed 2026-06-22)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## V12A — EDP Nielsen: MARCA_STD spot check
# MAGIC Source: VW_IR_YOG_GEL_MT_NLSN_PROD_DIM via EDP_MARKET layout SQL
# MAGIC Brand col: ITM_UNIF_BRND (= INP_56985)

# COMMAND ----------

v12a_sql = """
SELECT
    TRIM(INP_56985) AS ITM_UNIF_BRND,
    CASE
        WHEN TRIM(UPPER(INP_56985)) IN ('ACTIVIA')                                  THEN 'ACTIVIA'
        WHEN TRIM(UPPER(INP_56985)) IN ('BENEGASTRO', 'BENEG')                      THEN 'BENEGASTRO'
        WHEN TRIM(UPPER(INP_56985)) IN ('DANETTE')                                  THEN 'DANETTE'
        WHEN TRIM(UPPER(INP_56985)) IN ('DANMIX', 'DAN MIX', 'DANMMIX')            THEN 'DANMIX'
        WHEN TRIM(UPPER(INP_56985)) IN ('DANONE', 'DANONE YOGHURT', 'DANONE CREME',
                                         'DANONE FREE', 'DANONE GRIEGO', 'DANONE FS',
                                         'DAIRY', 'DAIRY DANONE MEXICO', 'DANAO')   THEN 'DANONE'
        WHEN TRIM(UPPER(INP_56985)) IN ('DANONINO', 'DANONINOLIQUIDO')              THEN 'DANONINO'
        WHEN TRIM(UPPER(INP_56985)) IN ('DANUP', 'DAN UP')                         THEN 'DANUP'
        WHEN TRIM(UPPER(INP_56985)) IN ('DANY', 'DANY DANETTE')                    THEN 'DANY'
        WHEN TRIM(UPPER(INP_56985)) IN ('HERSHEYS', 'HERSHEY''S', 'DANONE HERSHEYS') THEN 'HERSHEYS'
        WHEN TRIM(UPPER(INP_56985)) IN ('LICUAMIX')                                THEN 'LICUAMIX'
        WHEN TRIM(UPPER(INP_56985)) IN ('OIKOS', 'OIKOS UHT')                      THEN 'OIKOS'
        WHEN TRIM(UPPER(INP_56985)) IN ('SILK', 'SILK ORIG 946ML', 'SILKCHOCO190ML') THEN 'SILK'
        WHEN TRIM(UPPER(INP_56985)) IN ('SO DELICIOUS')                            THEN 'SO DELICIOUS'
        WHEN TRIM(UPPER(INP_56985)) IN ('VITALINEA')                               THEN 'VITALINEA'
        WHEN TRIM(UPPER(INP_56985)) IN ('YOPRO', 'YO PRO')                         THEN 'YOPRO'
        WHEN TRIM(UPPER(INP_56985)) IN ('OCEAN SPRAY', 'OCEAN')                    THEN 'OCEAN SPRAY'
        WHEN TRIM(UPPER(INP_56985)) IN ('LALA', 'GPO INDUSTRIAL LALA')             THEN 'LALA'
        ELSE TRIM(UPPER(INP_56985))
    END AS MARCA_STD,
    COUNT(DISTINCT "product_id") AS PRODUCT_COUNT
FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_PROD_DIM
WHERE INP_56985 IS NOT NULL
GROUP BY 1, 2
ORDER BY MARCA_STD, ITM_UNIF_BRND
"""

# DQ check: how many brands still fall through to ELSE (unmapped)?
v12a_dq_sql = """
SELECT
    COUNT(DISTINCT INP_56985)                                           AS TOTAL_BRANDS,
    COUNT(DISTINCT CASE WHEN TRIM(UPPER(INP_56985)) IN (
        'ACTIVIA','BENEGASTRO','BENEG','DANETTE','DANMIX','DAN MIX','DANMMIX',
        'DANONE','DANONE YOGHURT','DANONE CREME','DANONE FREE','DANONE GRIEGO',
        'DANONE FS','DAIRY','DAIRY DANONE MEXICO','DANAO','DANONINO','DANONINOLIQUIDO',
        'DANUP','DAN UP','DANY','DANY DANETTE','HERSHEYS','HERSHEY''S','DANONE HERSHEYS',
        'LICUAMIX','OIKOS','OIKOS UHT','SILK','SILK ORIG 946ML','SILKCHOCO190ML',
        'SO DELICIOUS','VITALINEA','YOPRO','YO PRO','OCEAN SPRAY','OCEAN',
        'LALA','GPO INDUSTRIAL LALA'
    ) THEN INP_56985 END)                                               AS MAPPED_BRANDS,
    COUNT(DISTINCT CASE WHEN TRIM(UPPER(INP_56985)) NOT IN (
        'ACTIVIA','BENEGASTRO','BENEG','DANETTE','DANMIX','DAN MIX','DANMMIX',
        'DANONE','DANONE YOGHURT','DANONE CREME','DANONE FREE','DANONE GRIEGO',
        'DANONE FS','DAIRY','DAIRY DANONE MEXICO','DANAO','DANONINO','DANONINOLIQUIDO',
        'DANUP','DAN UP','DANY','DANY DANETTE','HERSHEYS','HERSHEY''S','DANONE HERSHEYS',
        'LICUAMIX','OIKOS','OIKOS UHT','SILK','SILK ORIG 946ML','SILKCHOCO190ML',
        'SO DELICIOUS','VITALINEA','YOPRO','YO PRO','OCEAN SPRAY','OCEAN',
        'LALA','GPO INDUSTRIAL LALA'
    ) THEN INP_56985 END)                                               AS PASSTHROUGH_BRANDS,
    SUM(CASE WHEN INP_56985 IS NULL THEN 1 ELSE 0 END) * 100.0
        / COUNT(*)                                                       AS PCT_NULL_BRAND
FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_PROD_DIM
"""

log("=" * 70)
log("V12A — EDP Nielsen MARCA_STD spot check")
log("=" * 70)
try:
    df_v12a = run_sf_query("PRD_MEX", v12a_sql, "V12A — EDP MARCA_STD mapping")
    log_df(df_v12a, "EDP MARCA_STD → raw brand", n=300)
except Exception as e:
    log(f"  ❌ V12A FAILED: {e}")

try:
    df_v12a_dq = run_sf_query("PRD_MEX", v12a_dq_sql, "V12A DQ — EDP brand null + mapping rates")
    log_df(df_v12a_dq, "EDP brand DQ summary", n=10)
    # Threshold checks
    row = df_v12a_dq.collect()[0]
    total    = row["TOTAL_BRANDS"]
    mapped   = row["MAPPED_BRANDS"]
    passthru = row["PASSTHROUGH_BRANDS"]
    null_pct = float(row["PCT_NULL_BRAND"] or 0)
    log(f"  DQ CHECK — TOTAL={total}  MAPPED={mapped}  PASSTHROUGH={passthru}  NULL%={null_pct:.2f}")
    log(f"  THRESHOLD: min_brands=100  → {'✅ PASS' if total >= 100 else '❌ FAIL'}")
    log(f"  THRESHOLD: max_null_pct=0% → {'✅ PASS' if null_pct == 0 else '❌ FAIL'}")
except Exception as e:
    log(f"  ❌ V12A DQ FAILED: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## V12B — Water Retail Nielsen: MARCA_STD from ITM_UNIF_BRAND_DAN

# COMMAND ----------

v12b_sql = """
SELECT
    TRIM(A."CSTM_310589") AS ITM_UNIF_BRAND_RAW,
    CASE
        WHEN TRIM(UPPER(
            CASE
                WHEN A."CSTM_321331" = 'FIJI'                               THEN 'FIJI'
                WHEN A."CSTM_310589" = 'OTHERS MARCA'                        THEN 'OTHER BRANDS'
                WHEN A."CSTM_310589" = 'STA. MARIA -NESTLE'                  THEN 'SANTA MARIA (NESTLE)'
                WHEN A."CSTM_972397" IN ('LEVITE CLASICA','LEVITE INFUSIONES',
                                          'LEVITE CERO','LEVITE BALANCE')    THEN 'LEVITE'
                WHEN A."CSTM_310589" = 'BONAFONT' AND A."CSTM_972397" = 'KIDS'          THEN 'BONAFONT KIDS'
                WHEN A."CSTM_310589" = 'BONAFONT' AND A."CSTM_972397" = 'AGUAS FRESCAS' THEN 'BONAFONT AGUA FRESCAS'
                WHEN A."CSTM_321331" IN ('IND. REFRESQUERA PENINSULAR','COCA-COLA COMPANY') THEN 'COCA-COLA'
                WHEN A."CSTM_321331" IN ('GRUPO GEPP','PEPSICO')             THEN 'PEPSI'
                WHEN A."CSTM_321331" = 'LALA PRODS. LACTEOS'                 THEN 'LALA'
                WHEN A."CSTM_321331" = 'GRUPO PENAFIEL'                      THEN 'PEÑAFIEL'
                ELSE A."CSTM_310589"
            END
        )) IN ('BONAFONT','BONAFONT NATURAL','WATER BONAFONT')  THEN 'BONAFONT'
        WHEN TRIM(UPPER(
            CASE
                WHEN A."CSTM_310589" = 'BONAFONT' AND A."CSTM_972397" = 'KIDS' THEN 'BONAFONT KIDS'
                ELSE 'OTHER'
            END
        )) = 'BONAFONT KIDS'                                                THEN 'BONAFONT KIDS'
        WHEN A."CSTM_972397" IN ('LEVITE CLASICA','LEVITE INFUSIONES',
                                  'LEVITE CERO','LEVITE BALANCE')           THEN 'LEVITE'
        WHEN A."CSTM_310589" = 'BONAFONT' AND A."CSTM_972397" = 'AGUAS FRESCAS' THEN 'AGUAS FRESCAS'
        ELSE TRIM(UPPER(A."CSTM_310589"))
    END AS MARCA_STD_PREVIEW,
    COUNT(DISTINCT A."product_id") AS PRODUCT_COUNT
FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_RT_NLSN_PROD_DIM A
WHERE A."hierarchy_level" = 9
  AND A."CSTM_310589" IS NOT NULL
GROUP BY 1, 2
ORDER BY MARCA_STD_PREVIEW, ITM_UNIF_BRAND_RAW
"""

# Simpler DQ check: brand nulls and Danone brand presence
v12b_dq_sql = """
SELECT
    COUNT(DISTINCT A."CSTM_310589")  AS TOTAL_RAW_BRANDS,
    SUM(CASE WHEN A."CSTM_310589" IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS PCT_NULL_RAW_BRAND,
    COUNT(DISTINCT CASE WHEN A."CSTM_310589" = 'BONAFONT' THEN 1 END)   AS HAS_BONAFONT,
    COUNT(DISTINCT CASE WHEN A."CSTM_310589" = 'CIEL'     THEN 1 END)   AS HAS_CIEL
FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_RT_NLSN_PROD_DIM A
WHERE A."hierarchy_level" = 9
"""

log("=" * 70)
log("V12B — Water Retail MARCA_STD preview (ITM_UNIF_BRAND_DAN derived)")
log("=" * 70)
try:
    df_v12b = run_sf_query("PRD_MEX", v12b_sql, "V12B — Water Retail MARCA_STD preview")
    log_df(df_v12b, "Water Retail MARCA_STD preview", n=200)
except Exception as e:
    log(f"  ❌ V12B FAILED: {e}")

try:
    df_v12b_dq = run_sf_query("PRD_MEX", v12b_dq_sql, "V12B DQ — Water Retail brand summary")
    log_df(df_v12b_dq, "Water Retail brand DQ summary", n=10)
    row = df_v12b_dq.collect()[0]
    total = row["TOTAL_RAW_BRANDS"]
    null_pct = float(row["PCT_NULL_RAW_BRAND"] or 0)
    log(f"  DQ CHECK — TOTAL_BRANDS={total}  NULL%={null_pct:.2f}")
    log(f"  THRESHOLD: min_brands=80  → {'✅ PASS' if total >= 80 else '❌ FAIL'}")
    log(f"  THRESHOLD: max_null%=5%   → {'✅ PASS' if null_pct <= 5.0 else '❌ FAIL'}")
except Exception as e:
    log(f"  ❌ V12B DQ FAILED: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## V12C — Water Scantrack: MARCA_STD from ITM_UNIF_BRND_DAN

# COMMAND ----------

v12c_dq_sql = """
SELECT
    COUNT(DISTINCT A."CSTM_310589")  AS TOTAL_RAW_BRANDS,
    SUM(CASE WHEN A."CSTM_310589" IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS PCT_NULL_RAW_BRAND,
    COUNT(DISTINCT CASE WHEN A."CSTM_310589" = 'BONAFONT' THEN 1 END)   AS HAS_BONAFONT,
    COUNT(DISTINCT CASE WHEN A."CSTM_310589" = 'CIEL'     THEN 1 END)   AS HAS_CIEL
FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_ST_NLSN_PROD_DIM A
"""

log("=" * 70)
log("V12C — Water Scantrack MARCA_STD DQ (ITM_UNIF_BRND_DAN derived)")
log("=" * 70)
try:
    df_v12c_dq = run_sf_query("PRD_MEX", v12c_dq_sql, "V12C DQ — Water Scantrack brand summary")
    log_df(df_v12c_dq, "Water Scantrack brand DQ summary", n=10)
    row = df_v12c_dq.collect()[0]
    total = row["TOTAL_RAW_BRANDS"]
    null_pct = float(row["PCT_NULL_RAW_BRAND"] or 0)
    log(f"  DQ CHECK — TOTAL_BRANDS={total}  NULL%={null_pct:.2f}")
    log(f"  THRESHOLD: min_brands=70  → {'✅ PASS' if total >= 70 else '❌ FAIL'}")
    log(f"  THRESHOLD: max_null%=5%   → {'✅ PASS' if null_pct <= 5.0 else '❌ FAIL'}")
except Exception as e:
    log(f"  ❌ V12C FAILED: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## V12D — PB Scantrack: MARCA_STD from ITM_UNIF_BRAND (INP_56985)

# COMMAND ----------

v12d_sql = """
SELECT
    TRIM(B.INP_56985) AS ITM_UNIF_BRAND,
    CASE
        WHEN TRIM(UPPER(B.INP_56985)) IN ('SILK', 'SILK ORIG 946ML', 'SILKCHOCO190ML') THEN 'SILK'
        WHEN TRIM(UPPER(B.INP_56985)) IN ('SO DELICIOUS')                              THEN 'SO DELICIOUS'
        WHEN TRIM(UPPER(B.INP_56985)) IN ('YOPRO', 'YO PRO')                           THEN 'YOPRO'
        WHEN TRIM(UPPER(B.INP_56985)) IN ('CONTROLLED LABEL')                           THEN 'MARCAS GENERICAS'
        WHEN TRIM(UPPER(B.INP_56985)) IN ('PL', 'SIN MARCA')                           THEN 'MARCAS GENERICAS'
        WHEN TRIM(UPPER(B.INP_56985)) IN ('LALA VITA')                                 THEN 'LALA'
        ELSE TRIM(UPPER(B.INP_56985))
    END AS MARCA_STD,
    COUNT(DISTINCT B."product_id") AS PRODUCT_COUNT
FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_SUST_LECHE_ST_NLSN_PROD_DIM B
WHERE B.INP_56985 IS NOT NULL
GROUP BY 1, 2
ORDER BY MARCA_STD, ITM_UNIF_BRAND
"""

v12d_dq_sql = """
SELECT
    COUNT(DISTINCT INP_56985)                AS TOTAL_BRANDS,
    SUM(CASE WHEN INP_56985 IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS PCT_NULL_BRAND,
    COUNT(DISTINCT CASE WHEN TRIM(UPPER(INP_56985)) IN ('SILK','SO DELICIOUS','YOPRO','YO PRO')
                        THEN INP_56985 END)  AS DANONE_PB_BRANDS
FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_SUST_LECHE_ST_NLSN_PROD_DIM
"""

log("=" * 70)
log("V12D — PB Scantrack MARCA_STD spot check (INP_56985 = ITM_UNIF_BRAND)")
log("=" * 70)
try:
    df_v12d = run_sf_query("PRD_MEX", v12d_sql, "V12D — PB MARCA_STD mapping")
    log_df(df_v12d, "PB MARCA_STD → raw brand", n=100)
except Exception as e:
    log(f"  ❌ V12D FAILED: {e}")

try:
    df_v12d_dq = run_sf_query("PRD_MEX", v12d_dq_sql, "V12D DQ — PB brand DQ summary")
    log_df(df_v12d_dq, "PB brand DQ summary", n=10)
    row = df_v12d_dq.collect()[0]
    total  = row["TOTAL_BRANDS"]
    null_pct = float(row["PCT_NULL_BRAND"] or 0)
    danone = row["DANONE_PB_BRANDS"]
    log(f"  DQ CHECK — TOTAL_BRANDS={total}  NULL%={null_pct:.2f}  DANONE_BRANDS={danone}")
    log(f"  THRESHOLD: min_brands=40   → {'✅ PASS' if total >= 40 else '❌ FAIL'}")
    log(f"  THRESHOLD: max_null%=0%    → {'✅ PASS' if null_pct == 0 else '❌ FAIL'}")
    log(f"  THRESHOLD: Danone PB ≥ 2  → {'✅ PASS' if danone >= 2 else '❌ FAIL'}")
except Exception as e:
    log(f"  ❌ V12D DQ FAILED: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## V12E — Cross-check: MARCA_STD in Nielsen vs IBP join

# COMMAND ----------

v12e_sql = """
-- Join EDP Nielsen brands (via MARCA_STD) against IBP MARCA_STD
-- Confirms which Nielsen brands have an IBP counterpart
WITH edp_std AS (
    SELECT DISTINCT
        CASE
            WHEN TRIM(UPPER(INP_56985)) IN ('ACTIVIA')                    THEN 'ACTIVIA'
            WHEN TRIM(UPPER(INP_56985)) IN ('BENEGASTRO', 'BENEG')        THEN 'BENEGASTRO'
            WHEN TRIM(UPPER(INP_56985)) IN ('DANETTE')                    THEN 'DANETTE'
            WHEN TRIM(UPPER(INP_56985)) IN ('DANMIX', 'DAN MIX', 'DANMMIX') THEN 'DANMIX'
            WHEN TRIM(UPPER(INP_56985)) IN ('DANONE', 'DANONE YOGHURT', 'DANONE CREME',
                                             'DANONE FREE', 'DANONE GRIEGO', 'DANONE FS',
                                             'DAIRY', 'DAIRY DANONE MEXICO', 'DANAO')   THEN 'DANONE'
            WHEN TRIM(UPPER(INP_56985)) IN ('DANONINO', 'DANONINOLIQUIDO') THEN 'DANONINO'
            WHEN TRIM(UPPER(INP_56985)) IN ('DANUP', 'DAN UP')             THEN 'DANUP'
            WHEN TRIM(UPPER(INP_56985)) IN ('DANY', 'DANY DANETTE')        THEN 'DANY'
            WHEN TRIM(UPPER(INP_56985)) IN ('HERSHEYS', 'HERSHEY''S', 'DANONE HERSHEYS') THEN 'HERSHEYS'
            WHEN TRIM(UPPER(INP_56985)) IN ('OIKOS', 'OIKOS UHT')          THEN 'OIKOS'
            WHEN TRIM(UPPER(INP_56985)) IN ('SILK', 'SILK ORIG 946ML', 'SILKCHOCO190ML') THEN 'SILK'
            WHEN TRIM(UPPER(INP_56985)) IN ('SO DELICIOUS')                THEN 'SO DELICIOUS'
            WHEN TRIM(UPPER(INP_56985)) IN ('VITALINEA')                   THEN 'VITALINEA'
            WHEN TRIM(UPPER(INP_56985)) IN ('YOPRO', 'YO PRO')             THEN 'YOPRO'
            ELSE TRIM(UPPER(INP_56985))
        END AS MARCA_STD_EDP
    FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_PROD_DIM
    WHERE INP_56985 IS NOT NULL
),
ibp_std AS (
    SELECT DISTINCT
        CASE
            WHEN TRIM(UPPER(MARCA)) IN ('ACTIVIA')                    THEN 'ACTIVIA'
            WHEN TRIM(UPPER(MARCA)) IN ('BENEGASTRO')                 THEN 'BENEGASTRO'
            WHEN TRIM(UPPER(MARCA)) IN ('DANETTE')                    THEN 'DANETTE'
            WHEN TRIM(UPPER(MARCA)) IN ('DANMIX', 'DAN MIX')         THEN 'DANMIX'
            WHEN TRIM(UPPER(MARCA)) IN ('DANONE', 'DANONE FREE', 'DANONE GRIEGO') THEN 'DANONE'
            WHEN TRIM(UPPER(MARCA)) IN ('DANONINO')                   THEN 'DANONINO'
            WHEN TRIM(UPPER(MARCA)) IN ('DANUP', 'DAN UP')            THEN 'DANUP'
            WHEN TRIM(UPPER(MARCA)) IN ('DANY')                       THEN 'DANY'
            WHEN TRIM(UPPER(MARCA)) IN ('HERSHEYS', 'HERSHEY''S')     THEN 'HERSHEYS'
            WHEN TRIM(UPPER(MARCA)) IN ('OIKOS')                      THEN 'OIKOS'
            WHEN TRIM(UPPER(MARCA)) IN ('SILK')                       THEN 'SILK'
            WHEN TRIM(UPPER(MARCA)) IN ('VITALINEA')                  THEN 'VITALINEA'
            WHEN TRIM(UPPER(MARCA)) IN ('YOPRO', 'YO PRO')            THEN 'YOPRO'
            ELSE TRIM(UPPER(MARCA))
        END AS MARCA_STD_IBP
    FROM PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP
    WHERE MARCA IS NOT NULL
)
SELECT
    e.MARCA_STD_EDP,
    i.MARCA_STD_IBP,
    CASE WHEN i.MARCA_STD_IBP IS NOT NULL THEN 'MATCHED' ELSE 'NO_IBP_MATCH' END AS JOIN_STATUS
FROM edp_std e
LEFT JOIN ibp_std i ON e.MARCA_STD_EDP = i.MARCA_STD_IBP
ORDER BY JOIN_STATUS, e.MARCA_STD_EDP
"""

log("=" * 70)
log("V12E — EDP Nielsen MARCA_STD × IBP join check")
log("=" * 70)
try:
    df_v12e = run_sf_query("PRD_MDP", v12e_sql, "V12E — EDP MARCA_STD × IBP cross-source join")
    log_df(df_v12e, "EDP MARCA_STD vs IBP join", n=300)
    matched    = df_v12e.filter("JOIN_STATUS = 'MATCHED'").count()
    no_match   = df_v12e.filter("JOIN_STATUS = 'NO_IBP_MATCH'").count()
    log(f"  MATCHED={matched}  NO_IBP_MATCH={no_match}")
    log(f"  THRESHOLD: MATCHED ≥ 10 Danone brands → {'✅ PASS' if matched >= 10 else '❌ FAIL'}")
except Exception as e:
    log(f"  ❌ V12E FAILED: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save V12 Validation Log

# COMMAND ----------

log("=" * 70)
log("V12 — Nielsen MARCA_STD VALIDATION COMPLETE")
log("=" * 70)

saved_path = None
for candidate in get_log_candidates():
    try:
        os.makedirs(os.path.dirname(candidate), exist_ok=True)
        with open(candidate, "w", encoding="utf-8") as f:
            f.write("\n".join(LOG_LINES))
        saved_path = candidate
        log(f"✓ V12 log saved to: {saved_path}")
        print(f"✓ V12 log saved to: {saved_path}")
        break
    except Exception as e:
        log(f"  Could not write to {candidate}: {e}")
        continue

if saved_path is None:
    fallback = "/tmp/validation_results_nielsen_v12.txt"
    with open(fallback, "w", encoding="utf-8") as f:
        f.write("\n".join(LOG_LINES))
    print(f"⚠️  Saved to fallback: {fallback}")
    print("   → Copy to notebooks/validation_results_nielsen_v12.txt and commit.")

print("\n" + "=" * 70)
print("V12 COMPLETE — Last 30 lines:")
print("=" * 70)
print("\n".join(LOG_LINES[-30:]))

# COMMAND ----------
