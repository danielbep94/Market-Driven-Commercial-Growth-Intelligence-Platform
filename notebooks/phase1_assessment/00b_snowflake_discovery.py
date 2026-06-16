# Databricks notebook source
# MAGIC %md
# MAGIC # 00b · Enterprise Source Discovery & Metadata Catalog
# MAGIC
# MAGIC ># MAGIC ## Purpose
# MAGIC This notebook replaces the original profiling-only discovery script and transforms it into
# MAGIC a full **Enterprise Metadata Catalog engine**. It runs 9 structured steps per source and
# MAGIC stores all results in a shared `results` dict consumed by `00c_enterprise_catalog_writer.py`.
# MAGIC
# MAGIC ## Steps per Source
# MAGIC | Step | Name | Output |
# MAGIC |------|------|--------|
# MAGIC | 1 | Diagnostic + INFORMATION_SCHEMA | Row count, column count, native SF types |
# MAGIC | 2 | Full Column Inventory | Every column: distinct count, null %, sample values, PK flag |
# MAGIC | 3 | Business Domain Profile | Value frequency tables for all business key columns |
# MAGIC | 4 | DQ Rules Validation | Configurable rules from `configs/dq_rules_catalog.yaml` |
# MAGIC | 5 | Temporal Coverage + Gap Detection | Min/max dates, distinct periods, weekly gap scan |
# MAGIC | 6 | Automatic Grain Detection | Smallest column combo achieving highest uniqueness % |
# MAGIC | 7 | Numeric Volume + Anomaly Flags | Min/max/sum/negatives for all numeric columns |
# MAGIC | 8 | Duplicate / PK Analysis | Row-level duplicate check on candidate grain |
# MAGIC | 9 | Enterprise Readiness Score | 6-dimension scoring replacing old 4-dim score |
# MAGIC
# MAGIC ## SQL Syntax Rule
# MAGIC The Snowflake Spark connector does **NOT** support `DATABASE.SCHEMA.TABLE` inside SQL.
# MAGIC - ✅ `SELECT * FROM VW_MKT_ECOMM WHERE anio >= 2024`
# MAGIC - ✅ `SELECT * FROM MDP_DSP.VW_MKT_ECOMM` ← cross-schema JOIN only
# MAGIC - ❌ `SELECT * FROM PRD_MDP.MDP_DSP.VW_MKT_ECOMM`
# MAGIC
# MAGIC The `db` key goes in connector options. SQL uses `SCHEMA.TABLE` or just `TABLE`.

# COMMAND ----------

# MAGIC %md ## ─── SECTION A: SOURCES CONFIGURATION ──────────────────────────────
# MAGIC
# MAGIC Configure each source below. Set `None` for sources not yet ready.
# MAGIC
# MAGIC **Required keys** per source dict:
# MAGIC - `"db"`:         Snowflake database (e.g. `"PRD_MDP"`)
# MAGIC - `"schema"`:     Default schema (e.g. `"MDP_DSP"`)
# MAGIC - `"sql"`:        Query — use `TABLE` or `SCHEMA.TABLE`, never `DB.SCHEMA.TABLE`
# MAGIC
# MAGIC **Optional keys** (all have safe defaults if omitted):
# MAGIC - `"grain_hint"`:          Human description of expected grain (e.g. `"FECHA × MARCA × CAMPANA"`)
# MAGIC - `"business_keys"`:       List of column names to profile with value-frequency tables (Step 3).
# MAGIC                            If omitted, the global BUSINESS_KEY_COLS fallback is used.
# MAGIC                            Missing columns are warned and skipped — never an error.
# MAGIC - `"date_col"`:            Exact column name containing the primary date/period.
# MAGIC                            If omitted, auto-detection via DATE_PRIORITY is used.
# MAGIC - `"date_format"`:         Source format, e.g. `"yyyy-MM-dd"`, `"yyyyMMdd"`, `"yyyy"`.
# MAGIC                            Required only when `date_requires_cast: true`.
# MAGIC - `"date_requires_cast"`:  `true` if the column is stored as STRING or INTEGER and needs
# MAGIC                            to_date() before temporal analysis. Defaults to `false`.
# MAGIC - `"grain_cols"`:          Explicit list of columns that define the table grain, e.g.
# MAGIC                            `["FECHA", "MARCA", "CAMPANA", "MEDIO"]`. When provided,
# MAGIC                            Step 6 tests the exact combination first (priority), then
# MAGIC                            explores additional candidates. If omitted, auto-discovery
# MAGIC                            from BUSINESS_KEY_COLS is used.

# COMMAND ----------

SOURCES = {
    # ── 1 · Investment / Marketing ──────────────────────────────────────────
    "DATA_MKT_ON": {
        "db": "PRD_MDP",
        "schema": "MDP_DSP",
        "sql": "SELECT * FROM VW_MKT_ECOMM WHERE anio >= 2024",
        "grain_hint":        "FECHA × MARCA × CAMPANA × MEDIO",
        "business_keys":     ["MARCA", "CADENA", "CANAL", "CAMPANA", "MEDIO", "PLATAFORMA", "OBJETIVO"],
        "date_col":          "FECHA",
        "date_format":       "yyyy-MM-dd",
        "date_requires_cast": False,
        "grain_cols":        ["FECHA", "MARCA", "CAMPANA", "MEDIO"],
    },

    "DATA_MKT_OFF": {
        "db": "PRD_MDP",
        "schema": "MDP_STG",
        "sql": "SELECT * FROM FACT_MEDIA_OFF WHERE anio >= 2024",
        "grain_hint":        "FECHA × MARCA × CAMPANA × MEDIO",
        "business_keys":     ["MARCA", "CADENA", "CANAL", "CAMPANA", "MEDIO"],
        "date_col":          "FECHA",
        "date_format":       "yyyy-MM-dd",
        "date_requires_cast": False,
        "grain_cols":        ["FECHA", "MARCA", "CAMPANA", "MEDIO"],
    },

    # ── 2 · Sell-Out ────────────────────────────────────────────────────────
    "DATA_SELL_OUT": {
        "db": "PRD_MDP",
        "schema": "MDP_STG",
        "sql": """
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
        AVG_SELL,
        CBU_ID
    FROM PRD_MDP.MDP_DSP.VW_FACT_SELL_OUT
    WHERE PER_ID >= 20250101
)

SELECT
    -- Period Catalog
    per."DAY" AS DAY,

    -- CBU Catalog
    cbu.CBU_DSC,
    cbu.CBU_SAP,

    -- Store Catalog
    st.CHAIN,
    st.FORMAT,
    st.SUBCHAIN,

    -- Product Catalog
    prod.INT_ID,
    prod.CBU_ID,

    -- Fact Metrics
    f.UPC,
    f.VOL_SELL_OUT,
    f.PCS_SELL_OUT,
    f.AMOUNT_SELL_OUT,
    f.VOL_INV,
    f.PCS_INV,
    f.AVG_SELL
FROM fact_filtered f
INNER JOIN PRD_MDP.MDP_DWH.V_D_PERIOD per
    ON f.PER_ID = per.PER_ID
INNER JOIN PRD_MDP.MDP_DSP.VW_D_STORE_RM st
    ON f.STORE = st.INT_ID
   AND f.CBU_ID = st.CBU_ID
INNER JOIN PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM prod
    ON TO_VARCHAR(f.UPC) = TO_VARCHAR(prod.INT_ID)
   AND f.CBU_ID = prod.CBU_ID
INNER JOIN PRD_MDP.MDP_DWH.VW_CBU_RM cbu
    ON prod.CBU_ID = cbu.CBU_ID
""",
        "grain_hint":        "DAY × CBU × CHAIN × FORMAT × SUBCHAIN × UPC",
        "business_keys":     ["CHAIN", "FORMAT", "SUBCHAIN", "UPC", "CBU_DSC"],
        # DAY comes from the period catalog join — it is a proper DATE column
        "date_col":          "DAY",
        "date_format":       "yyyy-MM-dd",
        "date_requires_cast": False,
        # Grain declared explicitly — avoids the 40-combo cap problem
        "grain_cols":        ["DAY", "UPC", "CHAIN", "FORMAT", "SUBCHAIN", "CBU_ID"],
    },

    # ── 3 · Sell-In ─────────────────────────────────────────────────────────
    "DATA_SELL_IN": {
        "db":                "PRD_MDP",
        "schema":            "MDP_STG",
        "sql":               """WITH CTE_DICCIONARIO AS (
    SELECT
        SAL_ORG_COD,
        NEW_CUS_IDT,
        NEW_CUS_NAM_DSC,
        OLD_CUS_IDT,
        OLD_CUS_NAM_DSC,
        NEW_CUS_CHL_ARE_DSC,
        OLD_CUS_CHL_ARE_DSC,
        NEW_CUS_ADR_CTY_DSC,
        OLD_CUS_ADR_CTY_DSC,
        NEW_CUS_SAL_RGN_COD,
        OLD_CUS_SAL_RGN_COD
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER(PARTITION BY OLD_CUS_IDT, SAL_ORG_COD ORDER BY OLD_CUS_IDT, SAL_ORG_COD) AS SE_REPITE
        FROM PRD_MEX.MEX_DSP_OTC.VW_D_CUSTOMER_DICTONARY
    ) Q
    WHERE SE_REPITE = 1
),

CTE_CONSOLIDADO AS (
    -- ==============================================================================
    -- 1) WATERS CBU
    -- ==============================================================================
    SELECT
        DATE_TRUNC(MONTH, TO_DATE(TO_VARCHAR(FAC.BIL_DAT), 'YYYYMMDD')) AS FECHA,
        IFNULL(DC1.NEW_CUS_IDT, CASE 
            WHEN IFNULL(DC.OLD_CUS_IDT, '-99') NOT IN ('-99') THEN DC.NEW_CUS_IDT 
            ELSE CLI.CUS_IDT 
        END) AS CLIENTE,
        CONCAT('0068', IFNULL(DC1.NEW_CUS_IDT, CASE 
            WHEN IFNULL(DC.OLD_CUS_IDT, '-99') NOT IN ('-99') THEN DC.NEW_CUS_IDT 
            ELSE CLI.CUS_IDT 
        END)) AS CLIENTE_ID,
        PRO.LV2_UMB_BRD_DSC       AS MARCA,
        CLI.CUS_SAL_RGN_DSC_EXT   AS REGION,
        CLI.CUS_SAL_PLT_COD       AS ID_CEDIS,
        CLI.CUS_SAL_PLT_DSC       AS CEDIS_DSC,
        CLI.PXY_CAT_1ST_DSC       AS CLUSTER,
        FAC.CBU                   AS CBU,
        PER.MONTH_ID              AS MES,
        SUM(FAC.LITER)            AS VOLUMEN,
        SUM(FAC.BIL_INV)          AS VALOR
    FROM PRD_MEX.MEX_DSP_OTC.VW_FACT_RNV AS FAC
    LEFT JOIN PRD_MEX.MEX_DSP_OTC.V_D_CLIENT AS CLI
        ON FAC.SHP_CUS_IDT = CLI.CUS_IDT
        AND FAC.SAL_ORG_COD = CLI.SAL_ORG_COD
    LEFT JOIN CTE_DICCIONARIO DC
        ON CLI.CUS_IDT = DC.OLD_CUS_IDT
        AND FAC.SAL_ORG_COD = DC.SAL_ORG_COD
    LEFT JOIN CTE_DICCIONARIO DC1
        ON CASE 
            WHEN IFNULL(DC.OLD_CUS_IDT, '-99') NOT IN ('-99') THEN DC.NEW_CUS_IDT 
            ELSE CLI.CUS_IDT 
        END = DC1.OLD_CUS_IDT
        AND FAC.SAL_ORG_COD = DC1.SAL_ORG_COD
    LEFT JOIN PRD_MEX.MEX_DSP_OTC.V_D_PERIOD AS PER
        ON FAC.BIL_DAT = PER.PER_ID
    LEFT JOIN PRD_MEX.MEX_DSP_OTC.V_D_ITEM AS PRO
        ON FAC.MAT_IDT = PRO.MAT_IDT
    WHERE FAC.CBU IN ('WATERS')
      AND CLI.CUS_UNI_COD = '1'
      AND FAC.BIL_DOC_TYP_COD NOT IN ('ZINT', 'ZPIO')
      AND FAC.CUS_IND_KEY_COD NOT IN ('ZMXH') 
      AND FAC.MAT_IDT NOT IN ('167435', '175017', '175018', '156735', '156737', '156738', '156759', '157441')
    GROUP BY
        DATE_TRUNC(MONTH, TO_DATE(TO_VARCHAR(FAC.BIL_DAT), 'YYYYMMDD')),
        IFNULL(DC1.NEW_CUS_IDT, CASE WHEN IFNULL(DC.OLD_CUS_IDT, '-99') NOT IN ('-99') THEN DC.NEW_CUS_IDT ELSE CLI.CUS_IDT END),
        CONCAT('0068', IFNULL(DC1.NEW_CUS_IDT, CASE WHEN IFNULL(DC.OLD_CUS_IDT, '-99') NOT IN ('-99') THEN DC.NEW_CUS_IDT ELSE CLI.CUS_IDT END)),
        PRO.LV2_UMB_BRD_DSC,
        CLI.CUS_SAL_RGN_DSC_EXT,
        CLI.CUS_SAL_PLT_COD,
        CLI.CUS_SAL_PLT_DSC,
        CLI.PXY_CAT_1ST_DSC,
        FAC.CBU,
        PER.MONTH_ID

    UNION ALL

    -- ==============================================================================
    -- 2) CBU EDP
    -- ==============================================================================
    SELECT
        DATE_TRUNC(MONTH, TO_DATE(TO_VARCHAR(FAC.BIL_DAT), 'YYYYMMDD')) AS FECHA,
        IFNULL(DC1.NEW_CUS_IDT, CASE 
            WHEN IFNULL(DC.OLD_CUS_IDT, '-99') NOT IN ('-99') THEN DC.NEW_CUS_IDT 
            ELSE CLI.CUS_IDT 
        END) AS CLIENTE,
        CONCAT('0049', IFNULL(DC1.NEW_CUS_IDT, CASE 
            WHEN IFNULL(DC.OLD_CUS_IDT, '-99') NOT IN ('-99') THEN DC.NEW_CUS_IDT 
            ELSE CLI.CUS_IDT 
        END)) AS CLIENTE_ID,
        PRO.LV2_UMB_BRD_DSC         AS MARCA,
        CLI.CUS_SAL_RGN_DSC_EXT     AS REGION,
        CLI.CUS_SAL_PLT_COD         AS ID_CEDIS,
        CLI.CUS_SAL_PLT_DSC         AS CEDIS_DSC,
        CLI.PXY_CAT_1ST_DSC         AS CLUSTER,
        FAC.CBU                     AS CBU,
        PER.MONTH_ID                AS MES,
        SUM(FAC.BIL_NET_KGR / 1000) AS VOLUMEN,
        SUM(FAC.BIL_INV)            AS VALOR
    FROM PRD_MEX.MEX_DSP_OTC.VW_FACT_RNV AS FAC
    LEFT JOIN PRD_MEX.MEX_DSP_OTC.V_D_CLIENT AS CLI
        ON FAC.SHP_CUS_IDT = CLI.CUS_IDT
        AND FAC.SAL_ORG_COD = CLI.SAL_ORG_COD
    LEFT JOIN CTE_DICCIONARIO DC
        ON CLI.CUS_IDT = DC.OLD_CUS_IDT
        AND FAC.SAL_ORG_COD = DC.SAL_ORG_COD
    LEFT JOIN CTE_DICCIONARIO DC1
        ON CASE 
            WHEN IFNULL(DC.OLD_CUS_IDT, '-99') NOT IN ('-99') THEN DC.NEW_CUS_IDT 
            ELSE CLI.CUS_IDT 
        END = DC1.OLD_CUS_IDT
        AND FAC.SAL_ORG_COD = DC1.SAL_ORG_COD
    LEFT JOIN PRD_MEX.MEX_DSP_OTC.V_D_PERIOD AS PER
        ON FAC.BIL_DAT = PER.PER_ID
    LEFT JOIN PRD_MEX.MEX_DSP_OTC.V_D_ITEM AS PRO
        ON FAC.MAT_IDT = PRO.MAT_IDT
    WHERE FAC.SAL_ORG_COD IN ('0049')
      AND CLI.CUS_UNI_COD = '1'
      AND FAC.BIL_DOC_TYP_COD NOT IN ('ZINT', 'ZPIO')
      AND FAC.CUS_1ST_IND_COD NOT IN ('ZMX12')
      AND PRO.LV2_UMB_BRD_DSC NOT IN ('FERRERO', 'KINDER', 'MARS', 'CODISTRIBUCION')
    GROUP BY
        DATE_TRUNC(MONTH, TO_DATE(TO_VARCHAR(FAC.BIL_DAT), 'YYYYMMDD')),
        IFNULL(DC1.NEW_CUS_IDT, CASE WHEN IFNULL(DC.OLD_CUS_IDT, '-99') NOT IN ('-99') THEN DC.NEW_CUS_IDT ELSE CLI.CUS_IDT END),
        CONCAT('0049', IFNULL(DC1.NEW_CUS_IDT, CASE WHEN IFNULL(DC.OLD_CUS_IDT, '-99') NOT IN ('-99') THEN DC.NEW_CUS_IDT ELSE CLI.CUS_IDT END)),
        PRO.LV2_UMB_BRD_DSC,
        CLI.CUS_SAL_RGN_DSC_EXT,
        CLI.CUS_SAL_PLT_COD,
        CLI.CUS_SAL_PLT_DSC,
        CLI.PXY_CAT_1ST_DSC,
        FAC.CBU,
        PER.MONTH_ID
)

-- ==============================================================================
-- 3) FINAL SELECT WITH SINGLE DATE FILTER
-- ==============================================================================
SELECT *
FROM CTE_CONSOLIDADO
WHERE FECHA >= '2025-01-01';
""",
        "grain_hint":        "FECHA × SKU × CADENA",
        "business_keys":     ["SKU", "CADENA", "MARCA"],
        "date_col":          "FECHA",
        "date_format":       "yyyy-MM-dd",
        "date_requires_cast": False,
        "grain_cols":        ["FECHA", "SKU", "CADENA"],
    },
   
    # ── 4 · Waste / Merma ────────────────────────────────────────────────────
    "DATA_WASTE": {
        "db": "PRD_MDP",
        "schema": "MDP_STG",
        "sql": "SELECT * FROM VW_WASTE",
        # grain_hint, business_keys, date_col, and grain_cols will be filled in
        # after the first run reveals the actual columns in VW_WASTE.
        "grain_hint":        "[TO BE CONFIRMED after first run]",
        "business_keys":     ["SKU", "CADENA", "MARCA"],     # assumed — validate after Step 2
        "date_col":          "FECHA",                        # assumed — validate after Step 2
        "date_format":       "yyyy-MM-dd",
        "date_requires_cast": False,
        "grain_cols":        ["FECHA", "SKU", "CADENA"],     # assumed — validate after Step 6
    },

    # ── 5 · Demand Forecast ──────────────────────────────────────────────────
    "DATA_FORECAST": None,

    # ── 6 · Nielsen / Market Share ───────────────────────────────────────────
    "DATA_NIELSEN": None,

    # ── 7 · Price ────────────────────────────────────────────────────────────
    "DATA_PRICE": None,

    # ── 8 · Promotions ───────────────────────────────────────────────────────
    "DATA_PROMO": None,

    # ── 9 · Inventory / Stock ────────────────────────────────────────────────
    "DATA_INVENTORY": None,

    # ── 10 · Calendar / Date Dimension ──────────────────────────────────────
    "DATA_CALENDAR": None,
}

DOMAIN_LABELS = {
    "DATA_MKT_ON": "Investment / Marketing (Online)",
    "DATA_MKT_OFF": "Investment / Marketing (Offline)",
    "DATA_SELL_IN": "Sell-In",
    "DATA_SELL_OUT": "Sell-Out",
    "DATA_WASTE": "Waste / Merma",
    "DATA_FORECAST": "Demand Forecast",
    "DATA_NIELSEN": "Nielsen / Market Share",
    "DATA_PRICE": "Price",
    "DATA_PROMO": "Promotions",
    "DATA_INVENTORY": "Inventory / Stock",
    "DATA_CALENDAR": "Calendar / Date Dimension",
}

# COMMAND ----------

# MAGIC %md ## ─── SECTION B: DO NOT EDIT BELOW ───────────────────────────────────

# COMMAND ----------

# MAGIC %md ## B1 · Imports & Connection Setup

# COMMAND ----------
# MAGIC %run ../utils/execution_logger

# COMMAND ----------
import time as _time
_NB_START    = _time.time()
ENVIRONMENT = "dev"

# COMMAND ----------

import yaml, csv, os, itertools
from datetime import datetime
from collections import defaultdict

import pyspark.sql.functions as F
from pyspark.sql.types import NumericType, StringType, DateType, TimestampType
from pyspark.sql.window import Window

# ── Credentials ───────────────────────────────────────────────────────────────
KEYVAULT_NAME = "DAN-AM-P-KVT800-R-MDP-DB"
KEY_NAME_USR  = "snowflake-user"
KEY_NAME_PWD  = "snowflake-password"
SF_URL        = "danonenam.east-us-2.azure.snowflakecomputing.com"
SF_WAREHOUSE  = "PRD_MDP_ANL_WH"
SF_ROLE       = "PRD_MDP"

try:
    user     = dbutils.secrets.get(scope=KEYVAULT_NAME, key=KEY_NAME_USR)
    password = dbutils.secrets.get(scope=KEYVAULT_NAME, key=KEY_NAME_PWD)
    print(f"✅ Credentials loaded from: {KEYVAULT_NAME}")
except NameError:
    user, password = "MOCK_USER", "MOCK_PASSWORD"
    print("🚨 Non-Databricks env — using MOCK credentials")
except Exception as e:
    user, password = "MOCK_USER", "MOCK_PASSWORD"
    print(f"🚨 Secret error: {e}")

RUN_AT      = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
OUTPUT_DIR  = "docs/phase_outputs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Snowflake helpers ─────────────────────────────────────────────────────────
def get_sf_opts(db: str, schema: str) -> dict:
    return {
        "sfURL": SF_URL, "sfUser": user, "sfPassword": password,
        "sfDatabase": db, "sfSchema": schema, "sfWarehouse": SF_WAREHOUSE,
    }

def validate_source_config(source_key: str, cfg: dict):
    """Fail fast on incomplete source definitions instead of surfacing opaque connector errors."""
    if not isinstance(cfg, dict):
        raise ValueError(f"{source_key}: source config must be a dict or None")
    missing = [key for key in ("db", "schema", "sql") if not cfg.get(key)]
    if missing:
        raise ValueError(f"{source_key}: missing required config key(s): {', '.join(missing)}")

def run_sql(db: str, schema: str, sql: str):
    return spark.read.format("snowflake") \
               .options(**get_sf_opts(db, schema)) \
               .option("query", str(sql).strip()) \
               .load()

def run_info_schema(db: str, schema: str, object_name: str):
    """Pull native Snowflake types from INFORMATION_SCHEMA.COLUMNS."""
    try:
        info_sql = f"""
            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH,
                   NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE, ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = UPPER('{object_name}')
            ORDER BY ORDINAL_POSITION
        """
        df = run_sql(db, schema, info_sql)
        return {row["COLUMN_NAME"]: row.asDict() for row in df.collect()}
    except Exception as e:
        print(f"  ⚠️  INFORMATION_SCHEMA query failed: {str(e)[:150]}")
        return {}

# COMMAND ----------

# MAGIC %md ## B2 · Load Config Files

# COMMAND ----------

# ── DQ Rules ─────────────────────────────────────────────────────────────────
DQ_RULES = {}
try:
    with open("configs/dq_rules_catalog.yaml", "r") as f:
        DQ_RULES = yaml.safe_load(f) or {}
    print(f"✅ DQ rules loaded — {len(DQ_RULES)} datasets configured")
except Exception as e:
    print(f"⚠️  Could not load dq_rules_catalog.yaml: {e}")

# ── Business Glossary Seed ────────────────────────────────────────────────────
GLOSSARY_SEED = {}
try:
    with open("configs/business_glossary_seed.yaml", "r") as f:
        seed_data = yaml.safe_load(f) or {}
    for entry in seed_data.get("columns", []):
        GLOSSARY_SEED[entry["column_name"].upper()] = entry
    print(f"✅ Glossary seed loaded — {len(GLOSSARY_SEED)} column definitions")
except Exception as e:
    print(f"⚠️  Could not load business_glossary_seed.yaml: {e}")

# COMMAND ----------

# MAGIC %md ## B3 · Business Key Taxonomy (for Domain Profile & Dimension Discovery)

# COMMAND ----------

# ── B3a: Global fallback — used when a source does not declare "business_keys" ────
# Matching is case-insensitive: column names are lowercased before comparison.
# Add names here to expand profiling across ALL sources that lack a declaration.
BUSINESS_KEY_COLS_FALLBACK = {
    "marca", "cadena", "chain", "canal", "sku", "upc", "format", "formato",
    "categoria", "subcategoria", "campana", "medio", "plataforma", "objetivo",
    "subchain", "int_id", "cbu_code", "brand", "category", "subcategory",
}

def resolve_business_keys(cfg: dict, df_columns: list) -> tuple:
    """
    Resolve the list of business-key columns to profile for a given source.

    Resolution order:
    1. Source-level "business_keys" list (Section A config)  ← preferred
    2. Global BUSINESS_KEY_COLS_FALLBACK set                 ← automatic fallback

    For source-level declarations:
    - Each declared column is validated against df_columns (case-insensitive).
    - Columns that exist are returned as-is (preserving original casing from the DataFrame).
    - Columns that do NOT exist are logged as warnings and skipped — never an error.

    Returns:
        (resolved_cols, missing_cols)
        resolved_cols: list of column names (as they appear in the DataFrame)
        missing_cols:  list of declared names that were not found
    """
    declared = cfg.get("business_keys")      # None if key absent
    col_map  = {c.upper(): c for c in df_columns}   # UPPER → original casing

    if declared is not None:
        # Source-level declaration: validate each entry
        resolved, missing = [], []
        for name in declared:
            canonical = col_map.get(name.upper())
            if canonical is not None:
                resolved.append(canonical)
            else:
                missing.append(name)
        return resolved, missing
    else:
        # Fallback: match by lowercase name against global set
        resolved = [c for c in df_columns if c.lower() in BUSINESS_KEY_COLS_FALLBACK]
        return resolved, []

# ── B3b: Dimension taxonomy (EA4) — unchanged ────────────────────────────────
DIM_TAXONOMY = {
    "DIM_PRODUCT":   {"upc", "sku", "int_id", "cbu_code", "descripcion", "marca",
                      "product_id", "cod_producto", "ean", "gtin"},
    "DIM_CUSTOMER":  {"cadena", "chain", "subchain", "format", "canal", "customer_id",
                      "cliente", "retailer", "store_id", "tienda"},
    "DIM_DATE":      {"fecha", "fecha_proceso", "fecha_venta", "fecha_cierre",
                      "day_id", "anio", "mes", "year_id", "dt", "date", "week",
                      "semana", "periodo", "period", "year", "month"},
    "DIM_BRAND":     {"marca", "brand", "categoria", "subcategoria", "category",
                      "subcategory", "negocio", "business"},
    "DIM_MARKETING": {"campana", "campaign", "medio", "media", "plataforma",
                      "platform", "objetivo", "objective", "formato"},
}

def classify_dimension(col_lower: str) -> str:
    for dim, keywords in DIM_TAXONOMY.items():
        if col_lower in keywords:
            return dim
    return "FACT / MEASURE"

# ── B3c: Date column resolution ───────────────────────────────────────────────
# Auto-detection fallback list — used when "date_col" is not declared in SOURCES.
DATE_PRIORITY = ["anio", "año", "year", "yr", "periodo", "period",
                 "fecha", "fecha_proceso", "fecha_venta", "fecha_cierre",
                 "date", "dt", "week", "semana", "mes", "month", "day_id"]

def resolve_date_col(cfg: dict, df_columns: list) -> tuple:
    """
    Resolve the date column and whether it needs casting.

    Resolution order:
    1. cfg["date_col"]  — explicit declaration, validated against df_columns.
       If declared but not found → warning, falls back to auto-detection.
    2. DATE_PRIORITY auto-detection (case-insensitive match).

    Returns:
        (date_col, requires_cast, date_format, resolution_method)
        date_col:         column name as it appears in the DataFrame, or None
        requires_cast:    True if to_date() must be called before temporal analysis
        date_format:      Spark date format string (e.g. "yyyyMMdd") or None
        resolution_method: "DECLARED" | "AUTO_DETECTED" | "NOT_FOUND"
    """
    declared_name   = cfg.get("date_col")
    requires_cast   = cfg.get("date_requires_cast", False)
    date_format     = cfg.get("date_format")
    col_map         = {c.upper(): c for c in df_columns}

    if declared_name is not None:
        canonical = col_map.get(declared_name.upper())
        if canonical is not None:
            return canonical, requires_cast, date_format, "DECLARED"
        else:
            print(f"  ⚠️  Declared date_col '{declared_name}' not found in DataFrame. "
                  f"Falling back to auto-detection.")
            # fall through to auto-detection

    col_lower_map = {c.lower(): c for c in df_columns}
    for p in DATE_PRIORITY:
        if p in col_lower_map:
            return col_lower_map[p], False, None, "AUTO_DETECTED"

    return None, False, None, "NOT_FOUND"

def get_date_series(df, date_col: str, requires_cast: bool, date_format: str):
    """
    Return a DataFrame column expression for temporal analysis.
    If requires_cast is True, wraps the column in to_date(col, format).
    Returns a tuple (df_with_date, working_col_name) where working_col_name
    is the alias used in subsequent agg/window expressions.
    """
    WORKING_ALIAS = "__date_working__"
    if requires_cast and date_format:
        df_with = df.withColumn(WORKING_ALIAS,
                                F.to_date(qcol(date_col), date_format))
    else:
        df_with = df.withColumn(WORKING_ALIAS, qcol(date_col).cast("date"))
    return df_with, WORKING_ALIAS

# COMMAND ----------

# MAGIC %md ## B4 · Main Discovery Loop

# COMMAND ----------

import itertools
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import NumericType, DateType, TimestampType

# Define the missing column-quoting helper function to handle special characters
qcol = lambda c: F.col(f"`{c}`")

defined   = {k: v for k, v in SOURCES.items() if v is not None}
undefined = {k: v for k, v in SOURCES.items() if v is None}

print(f"Sources defined:  {len(defined)}/10")
print(f"Sources pending:  {len(undefined)}/10")
if undefined:
    print(f"  Pending: {', '.join(undefined.keys())}")

results = {}  # Shared across this notebook and 00c

for source_key, cfg in defined.items():
    label = DOMAIN_LABELS.get(source_key, source_key)
    db, schema, sql = cfg["db"], cfg["schema"], cfg["sql"]
    grain_hint = cfg.get("grain_hint", "")

    print("\n" + "═" * 72)
    print(f"  {source_key}  —  {label}")
    print("═" * 72)
    print(f"  db={db}  schema={schema}")
    print(f"  SQL: {sql.strip()[:100]}{'...' if len(sql.strip()) > 100 else ''}\n")

    res = {
        # Identity
        "key": source_key, "label": label, "db": db, "schema": schema, "sql": sql,
        "grain_hint": grain_hint, "status": "OK",
        # Counts
        "total_rows": 0, "total_cols": 0,
        # Step 1
        "sf_types": {},
        # Step 2 — Full column inventory
        "col_inventory": [],
        # Step 3 — Business domain profile
        "domain_profile": [],
        # Step 4 — DQ rules
        "dq_results": [],
        "dq_pass_count": 0, "dq_fail_count": 0,
        # Step 5 — Temporal
        "date_col": None, "date_min": None, "date_max": None,
        "date_distinct": None, "temporal_gaps": 0,
        # Step 6 — Grain
        "grain_candidates": [],
        # Step 7 — Numeric
        "numeric_stats": [],
        # Step 8 — Duplicates
        "dup_count": 0, "dup_pct": 0.0, "dup_key_used": [],
        # Step 9 — Enterprise score
        "score_completeness": 0, "score_consistency": 0, "score_joinability": 0,
        "score_temporal": 0, "score_documentation": 0, "score_grain": 0,
        "enterprise_score": 0,
        # Misc
        "errors": [],
    }

    # ── Step 1: Execute + INFORMATION_SCHEMA ─────────────────────────────────
    try:
        validate_source_config(source_key, cfg)
        df = run_sql(db, schema, sql).cache()
        res["total_rows"] = df.count()
        res["total_cols"] = len(df.columns)
        print(f"  ✅ Step 1 — OK  |  Rows: {res['total_rows']:,}  |  Cols: {res['total_cols']}")
    except Exception as e:
        err_msg = str(e)
        res["status"] = "ERROR"
        res["errors"].append(err_msg[:300])
        print(f"  ❌ Step 1 — FAILED: {err_msg[:300]}")
        results[source_key] = res
        # Per-source log: TABLE_NOT_FOUND / CONNECTION_FAILED
        write_execution_log(
            notebook_id  = "00b_snowflake_discovery",
            source_name  = source_key,
            status       = "ERROR",
            duration_sec = _time.time() - _NB_START,
            errors       = [make_error(
                step          = "Step 1 — Execute SQL + Row Count",
                category      = "TABLE_NOT_FOUND",
                severity      = "CRITICAL",
                message       = err_msg[:300],
                raw_exception = err_msg,
                resolution    = "Verify db/schema/sql in SOURCES config for this source",
                is_blocking   = True,
            )],
            warnings     = [],
            metrics      = {"db": db, "schema": schema},
            output_files = [],
            environment  = ENVIRONMENT,
        )
        continue

    # Attempt INFORMATION_SCHEMA pull for native types
    view_name = sql.strip().split("FROM")[-1].strip().split()[0].split(".")[-1]
    res["sf_types"] = run_info_schema(db, schema, view_name)
    if res["sf_types"]:
        print(f"  ✅ INFORMATION_SCHEMA — {len(res['sf_types'])} native type definitions loaded")
    else:
        print(f"  ⚠️  Native Snowflake types not available — using PySpark types only")

    # ── Step 2: Full Column Inventory ────────────────────────────────────────
    print(f"\n  Step 2 — Full Column Inventory ({res['total_cols']} columns)")
    print(f"  {'Column':<45} {'SF Type':<20} {'Nullable':<10} {'Distinct':>10} {'Null %':>8} {'PK?':>6}")
    print("  " + "─" * 100)

    for field in df.schema.fields:
        col = field.name
        col_lower = col.lower()

        # Null analysis
        null_count   = df.filter(qcol(col).isNull()).count()
        null_pct     = round(null_count / res["total_rows"] * 100, 2) if res["total_rows"] > 0 else 0.0

        # Distinct count
        distinct_cnt = df.select(qcol(col)).distinct().count()

        # PK candidate: unique + never null
        is_pk_cand   = (distinct_cnt == res["total_rows"] and null_pct == 0.0)

        # Sample values: top 5 by frequency (capped at 200 chars)
        try:
            top_vals = (
                df.groupBy(qcol(col)).count()
                  .orderBy(F.desc("count"))
                  .limit(5)
                  .select(qcol(col))
                  .rdd.flatMap(lambda x: x)
                  .collect()
            )
            sample_str = " | ".join([str(v) for v in top_vals if v is not None])[:200]
        except Exception:
            sample_str = ""

        # Native Snowflake type
        sf_info   = res["sf_types"].get(col.upper(), {})
        sf_type   = sf_info.get("DATA_TYPE", str(field.dataType))

        # Dimension classification
        dim_class = classify_dimension(col_lower)

        # Glossary linkage
        in_glossary = col.upper() in GLOSSARY_SEED

        null_flag = "⚠️ HIGH" if null_pct > 5 else ("🔶 WARN" if null_pct > 1 else "✅ OK")

        print(f"  {col:<45} {sf_type:<20} {'NULL' if field.nullable else 'NOT NULL':<10} "
              f"{distinct_cnt:>10,} {null_pct:>7.2f}% {'✓ PK' if is_pk_cand else '':>6}")

        res["col_inventory"].append({
            "dataset":        source_key,
            "column":         col,
            "pyspark_type":   str(field.dataType),
            "sf_type":        sf_type,
            "nullable":       field.nullable,
            "null_count":     null_count,
            "null_pct":       null_pct,
            "null_flag":      null_flag,
            "distinct_count": distinct_cnt,
            "is_pk_candidate": is_pk_cand,
            "sample_values":  sample_str,
            "dimension_class": dim_class,
            "in_glossary":    in_glossary,
        })

    # ── Step 3: Business Domain Profile ──────────────────────────────────────
    print(f"\n  Step 3 — Business Domain Profile")

    bus_key_found, bus_key_missing = resolve_business_keys(cfg, df.columns)
    resolution_mode = "source-declared" if cfg.get("business_keys") is not None else "global fallback"
    print(f"  Resolution mode:           {resolution_mode}")
    if bus_key_missing:
        for _m in bus_key_missing:
            print(f"  ⚠️  Declared business key '{_m}' not found in DataFrame — skipped")
        res["errors"].append(
            f"Step 3: declared business key(s) not found: {bus_key_missing}"
        )
    print(f"  Columns to profile ({len(bus_key_found)}): {bus_key_found or 'none'}")

    for col in bus_key_found:
        try:
            _null_count_step3 = df.filter(qcol(col).isNull()).count()
            _distinct_step3   = df.select(qcol(col)).distinct().count()
            _top_n            = min(50, _distinct_step3)   # never request more rows than exist

            freq_df = (
                df.groupBy(qcol(col)).count()
                  .withColumn("freq_pct", F.round(F.col("count") / res["total_rows"] * 100, 2))
                  .orderBy(F.desc("count"))
                  .limit(_top_n)
            )
            rows = freq_df.collect()
            print(f"\n    {col}  (distinct={_distinct_step3:,}  null={_null_count_step3:,})")
            print(f"      {'Value':<40} {'Count':>10}  {'Pct':>7}")
            print(f"      {'─'*40} {'─'*10}  {'─'*7}")
            for r in rows[:10]:
                val = str(r[col]) if r[col] is not None else "(null)"
                print(f"      {val:<40} {r['count']:>10,}  {r['freq_pct']:>6.2f}%")
            if len(rows) > 10:
                print(f"      ... ({len(rows) - 10} more values in CSV output)")

            for r in rows:
                res["domain_profile"].append({
                    "dataset":      source_key,
                    "column":       col,
                    "value":        str(r[col]) if r[col] is not None else "(null)",
                    "frequency":    r["count"],
                    "coverage_pct": r["freq_pct"],
                })
        except Exception as e:
            print(f"    ⚠️  Could not profile {col}: {str(e)[:100]}")

    if not bus_key_found:
        _hint = ("Check 'business_keys' list in SOURCES config."
                 if cfg.get("business_keys") is not None
                 else "Add column names to BUSINESS_KEY_COLS_FALLBACK or declare 'business_keys' in SOURCES.")
        print(f"  ℹ️  No business key columns matched. {_hint}")

    # ── Step 4: DQ Rules Validation ──────────────────────────────────────────
    print(f"\n  Step 4 — DQ Rules Validation")
    source_rules = DQ_RULES.get(source_key, {})

    if not source_rules:
        print(f"  ℹ️  No DQ rules configured for {source_key} in dq_rules_catalog.yaml")
    else:
        print(f"  {'Column':<40} {'Rule':<20} {'Expected':<20} {'Actual':<20} Status")
        print("  " + "─" * 110)

    for col_name, rules in source_rules.items():
        col_exists = col_name in df.columns
        severity   = rules.get("severity", "MEDIUM")

        for rule_type, rule_val in rules.items():
            if rule_type in ("severity", "description"):
                continue

            expected_str = ""
            actual_str   = ""
            status       = "PASS"

            try:
                if rule_type == "mandatory":
                    if not col_exists:
                        status = "FAIL — COLUMN MISSING"
                        expected_str = "0% null"
                        actual_str   = "column absent"
                    else:
                        col_entry = next((c for c in res["col_inventory"] if c["column"] == col_name), {})
                        npct = col_entry.get("null_pct", 0)
                        expected_str = "0.00% null"
                        actual_str   = f"{npct:.2f}% null"
                        status = "PASS" if npct == 0 else f"FAIL — {npct:.2f}% null"

                elif rule_type == "allowed_values" and col_exists:
                    allowed = rule_val
                    bad_count = df.filter(~qcol(col_name).isin(allowed) & qcol(col_name).isNotNull()).count()
                    expected_str = f"0 violations"
                    actual_str   = f"{bad_count:,} violations"
                    status = "PASS" if bad_count == 0 else f"FAIL — {bad_count:,} bad values"

                elif rule_type == "value_range" and col_exists:
                    mn = rule_val.get("min")
                    mx = rule_val.get("max")
                    oob = 0
                    if mn is not None:
                        oob += df.filter(qcol(col_name) < mn).count()
                    if mx is not None:
                        oob += df.filter(qcol(col_name) > mx).count()
                    expected_str = f"[{mn}, {mx}]"
                    actual_str   = f"{oob:,} out-of-range"
                    status = "PASS" if oob == 0 else f"FAIL — {oob:,} OOB rows"

                elif rule_type == "unique" and rule_val and col_exists:
                    col_entry = next((c for c in res["col_inventory"] if c["column"] == col_name), {})
                    is_unique = col_entry.get("is_pk_candidate", False)
                    expected_str = "all distinct"
                    actual_str   = f"{col_entry.get('distinct_count', '?')} distinct / {res['total_rows']:,} rows"
                    status = "PASS" if is_unique else "FAIL — duplicates exist"

            except Exception as e:
                status = f"ERROR: {str(e)[:60]}"

            pass_flag = "PASS" in status
            if pass_flag:
                res["dq_pass_count"] += 1
            else:
                res["dq_fail_count"] += 1

            severity_icon = {"CRITICAL": "🔴", "HIGH": "⚠️ ", "MEDIUM": "🔶", "LOW": "ℹ️ "}.get(severity, "")
            print(f"  {col_name:<40} {rule_type:<20} {expected_str:<20} {actual_str:<20} "
                  f"{'✅' if pass_flag else severity_icon} {status}")

            res["dq_results"].append({
                "dataset":      source_key,
                "column":       col_name,
                "rule_type":    rule_type,
                "severity":     severity,
                "expected":     expected_str,
                "actual":       actual_str,
                "status":       status,
                "passed":       pass_flag,
            })

    total_dq = res["dq_pass_count"] + res["dq_fail_count"]
    if total_dq > 0:
        print(f"\n  → DQ Summary: {res['dq_pass_count']}/{total_dq} rules PASS  |  "
              f"{res['dq_fail_count']} FAIL")

    # ── Step 5: Temporal Coverage + Gap Detection ─────────────────────────────
    print(f"\n  Step 5 — Temporal Coverage")
    date_col, _requires_cast, _date_fmt, _resolution = resolve_date_col(cfg, df.columns)
    res["date_col"] = date_col

    if date_col:
        print(f"  Date column: '{date_col}'  (resolution={_resolution}  "
              f"requires_cast={_requires_cast}  format={_date_fmt or 'native'})")

        # Build a date-typed series for all temporal operations
        try:
            df_dated, _working = get_date_series(df, date_col, _requires_cast, _date_fmt)
        except Exception as _e:
            print(f"  ❌ Date cast failed: {str(_e)[:150]}")
            print(f"     Fix: set 'date_requires_cast' and 'date_format' correctly in SOURCES config.")
            res["errors"].append(f"Step 5: date cast failed: {str(_e)[:150]}")
            df_dated, _working = df, date_col   # fall back to raw column

        # Check for invalid/null dates after cast
        _null_after_cast = df_dated.filter(F.col(_working).isNull()).count()
        if _null_after_cast > 0 and _requires_cast:
            _null_pct_date = round(_null_after_cast / res["total_rows"] * 100, 2)
            print(f"  ⚠️  {_null_after_cast:,} rows ({_null_pct_date}%) produced NULL after "
                  f"to_date('{date_col}', '{_date_fmt}') — likely malformed values or wrong format.")
            res["errors"].append(
                f"Step 5: {_null_after_cast} null dates after cast — verify date_format"
            )

        stats = df_dated.agg(
            F.min(F.col(_working)).alias("min"),
            F.max(F.col(_working)).alias("max"),
            F.countDistinct(F.col(_working)).alias("distinct")
        ).collect()[0]
        res["date_min"]      = str(stats["min"])
        res["date_max"]      = str(stats["max"])
        res["date_distinct"] = stats["distinct"]
        print(f"    Range:   {res['date_min']} → {res['date_max']}")
        print(f"    Periods: {res['date_distinct']:,} distinct values")

        # Weekly gap detection — always works now because _working is always cast to date
        try:
            w = Window.orderBy("_week")
            gap_df = (
                df_dated
                  .withColumn("_week", F.date_trunc("week", F.col(_working)))
                  .groupBy("_week").agg(F.count("*").alias("cnt"))
                  .withColumn("prev_week", F.lag("_week").over(w))
                  .withColumn("gap_weeks", F.datediff("_week", "prev_week") / 7)
                  .filter(F.col("gap_weeks") > 2)
            )
            res["temporal_gaps"] = gap_df.count()
            if res["temporal_gaps"] > 0:
                print(f"    ⚠️  {res['temporal_gaps']} gap(s) > 2 consecutive weeks detected:")
                gap_df.select("prev_week", "_week", "gap_weeks").show(5, truncate=False)
            else:
                print(f"    ✅ No temporal gaps > 2 consecutive weeks")
        except Exception as e:
            print(f"    ⚠️  Gap detection skipped: {str(e)[:100]}")
    else:
        print(f"  ⚠️  No date column found (resolution={_resolution})")
        print(f"     Fix: add 'date_col': '<column_name>' to this source's config in SOURCES.")
        print(f"     Available columns: {', '.join(df.columns[:15])}")

    # ── Step 6: Grain Detection ───────────────────────────────────────────────
    # Strategy:
    # A. If "grain_cols" is declared in the source config → test that exact combination
    #    first (priority pass). Then explore additional candidates for confirmation.
    # B. If "grain_cols" is absent → fall back to automatic pool-based brute-force.
    # The MAX_CHECKS cap only applies to brute-force candidates, not the declared grain.
    print(f"\n  Step 6 — Grain Detection")
    if grain_hint:
        print(f"  Grain hint (config):  '{grain_hint}'")

    declared_grain_cols = cfg.get("grain_cols")  # list or None
    col_map_upper = {c.upper(): c for c in df.columns}

    # ── 6a: Validate declared grain_cols ────────────────────────────────────
    declared_grain_resolved = []
    if declared_grain_cols:
        _missing_grain = []
        for _g in declared_grain_cols:
            _canonical = col_map_upper.get(_g.upper())
            if _canonical:
                declared_grain_resolved.append(_canonical)
            else:
                _missing_grain.append(_g)
        if _missing_grain:
            print(f"  ⚠️  Declared grain_cols not found in DataFrame: {_missing_grain}")
            print(f"     These columns are excluded from grain validation.")
            res["errors"].append(f"Step 6: declared grain_cols not found: {_missing_grain}")
        print(f"  Declared grain ({len(declared_grain_resolved)} cols): {declared_grain_resolved}")

    # ── 6b: Test declared grain first (priority pass) ────────────────────────
    best_uniqueness = 0.0
    _declared_result = None

    if declared_grain_resolved:
        try:
            deduped  = df.dropDuplicates(declared_grain_resolved).count()
            uniq_pct = round(deduped / res["total_rows"] * 100, 2) if res["total_rows"] > 0 else 0.0
            confidence = (
                "✅ PERFECT" if uniq_pct == 100
                else ("🟡 HIGH"   if uniq_pct >= 95
                else ("🔶 MEDIUM" if uniq_pct >= 80
                else "🔴 LOW"))
            )
            combo_str = " × ".join(declared_grain_resolved)
            print(f"\n  {'Candidate Combination':<60} {'Unique %':>10} {'Status':>12}")
            print("  " + "─" * 85)
            print(f"  [DECLARED] {combo_str:<49} {uniq_pct:>9.2f}%  {confidence}")
            _declared_result = {
                "dataset":         source_key,
                "candidate_grain": combo_str,
                "columns_used":    declared_grain_resolved,
                "combo_size":      len(declared_grain_resolved),
                "uniqueness_pct":  uniq_pct,
                "confidence":      confidence,
                "is_best":         False,
                "source":          "DECLARED",
            }
            res["grain_candidates"].append(_declared_result)
            best_uniqueness = uniq_pct

            if uniq_pct < 100:
                print(f"  ℹ️  Declared grain is not perfect ({uniq_pct}%). "
                      f"Investigating additional candidates below.")
        except Exception as e:
            print(f"  ⚠️  Declared grain test failed: {str(e)[:100]}")

    # ── 6c: Build exploration pool ────────────────────────────────────────────
    # Pool = business keys already found in Step 3 + date column + any declared grain cols
    # Exclude columns already tested as the declared grain to avoid redundancy.
    _grain_date_keywords = {
        "fecha", "day_id", "anio", "mes", "year_id", "fecha_proceso",
        "fecha_venta", "week", "semana", "periodo", "date", "dt"
    }
    grain_candidates_pool = [
        c for c in df.columns
        if c.lower() in (BUSINESS_KEY_COLS_FALLBACK | _grain_date_keywords)
        and c not in declared_grain_resolved
    ]
    if date_col and date_col not in grain_candidates_pool and date_col not in declared_grain_resolved:
        grain_candidates_pool.insert(0, date_col)

    # Add any declared grain columns not already in the pool (ensures they appear
    # in smaller sub-combinations as well)
    for _g in declared_grain_resolved:
        if _g not in grain_candidates_pool:
            grain_candidates_pool.insert(0, _g)

    grain_candidates_pool = grain_candidates_pool[:12]   # raised from 10 to 12

    if grain_candidates_pool and best_uniqueness < 100.0:
        if not declared_grain_resolved:
            print(f"\n  {'Candidate Combination':<60} {'Unique %':>10} {'Status':>12}")
            print("  " + "─" * 85)
        print(f"  Exploration pool ({len(grain_candidates_pool)} cols): {grain_candidates_pool}")

        MAX_CHECKS = 50   # raised from 40
        checked = 0

        for combo_size in range(1, min(6, len(grain_candidates_pool) + 1)):
            for combo in itertools.combinations(grain_candidates_pool, combo_size):
                if checked >= MAX_CHECKS:
                    break
                # Skip if this combo is a superset of the declared grain (already tested)
                if declared_grain_resolved and set(declared_grain_resolved).issubset(set(combo)):
                    continue
                checked += 1
                try:
                    deduped  = df.dropDuplicates(list(combo)).count()
                    uniq_pct = round(deduped / res["total_rows"] * 100, 2) if res["total_rows"] > 0 else 0.0
                    confidence = (
                        "✅ PERFECT" if uniq_pct == 100
                        else ("🟡 HIGH"   if uniq_pct >= 95
                        else ("🔶 MEDIUM" if uniq_pct >= 80
                        else "🔴 LOW"))
                    )
                    combo_str = " × ".join(combo)
                    print(f"  [AUTO]     {combo_str:<49} {uniq_pct:>9.2f}%  {confidence}")

                    res["grain_candidates"].append({
                        "dataset":         source_key,
                        "candidate_grain": combo_str,
                        "columns_used":    list(combo),
                        "combo_size":      combo_size,
                        "uniqueness_pct":  uniq_pct,
                        "confidence":      confidence,
                        "is_best":         False,
                        "source":          "AUTO",
                    })

                    if uniq_pct > best_uniqueness:
                        best_uniqueness = uniq_pct

                    if uniq_pct == 100.0:
                        break

                except Exception as e:
                    print(f"  ⚠️  Skipped {combo}: {str(e)[:80]}")

            if best_uniqueness == 100.0:
                break

    # ── 6d: Mark best candidate ───────────────────────────────────────────────
    if res["grain_candidates"]:
        best = max(res["grain_candidates"], key=lambda x: x["uniqueness_pct"])
        best["is_best"] = True
        _best_src = best.get("source", "")
        print(f"\n  ★ Best grain [{_best_src}]: '{best['candidate_grain']}'  "
              f"({best['uniqueness_pct']}%)")
        if best["uniqueness_pct"] < 95:
            print(f"  ⚠️  Best grain uniqueness is below 95%. "
                  f"Consider adding more columns to 'grain_cols' in SOURCES config.")

    # ── Step 7: Numeric Volume + Anomaly Flags ────────────────────────────────
    print(f"\n  Step 7 — Numeric Volume")
    num_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, NumericType)]
    print(f"  Numeric columns found: {len(num_cols)}")
    print(f"  {'Column':<40} {'Min':>15} {'Max':>15} {'Sum':>20} {'Negatives':>12}")
    print("  " + "─" * 105)

    for col in num_cols:
        try:
            stats = df.agg(
                F.min(qcol(col)).alias("mn"), F.max(qcol(col)).alias("mx"),
                F.sum(qcol(col)).alias("tot"), F.mean(qcol(col)).alias("avg")
            ).collect()[0]
            negs  = df.filter(qcol(col) < 0).count()
            zeros = df.filter(qcol(col) == 0).count()
            mn_v  = float(stats["mn"] or 0)
            mx_v  = float(stats["mx"] or 0)
            tot_v = float(stats["tot"] or 0)
            avg_v = float(stats["avg"] or 0)
            neg_flag = f"⚠️  {negs:,}" if negs > 0 else "✅ none"
            print(f"  {col:<40} {mn_v:>15,.1f} {mx_v:>15,.1f} {tot_v:>20,.0f} {neg_flag:>12}")
            res["numeric_stats"].append({
                "dataset": source_key, "col": col,
                "min": mn_v, "max": mx_v, "sum": tot_v, "avg": avg_v,
                "negatives": negs, "zeros": zeros,
            })
        except Exception as e:
            print(f"  ⚠️  {col}: {str(e)[:80]}")

    if not num_cols:
        print(f"  ℹ️  No numeric columns found in this dataset")

    # ── Step 8: Duplicate / PK Analysis ──────────────────────────────────────
    print(f"\n  Step 8 — Duplicate & PK Analysis")
    best_grain = next((g for g in res["grain_candidates"] if g.get("is_best")), None)
    dup_key = best_grain["columns_used"] if best_grain else []

    if len(dup_key) >= 2:
        try:
            deduped   = df.dropDuplicates(dup_key).count()
            dup_count = res["total_rows"] - deduped
            dup_pct   = round(dup_count / res["total_rows"] * 100, 2) if res["total_rows"] > 0 else 0.0
            res["dup_count"] = dup_count
            res["dup_pct"]   = dup_pct
            res["dup_key_used"] = dup_key
            flag = f"⚠️  {dup_count:,} duplicates ({dup_pct}%)" if dup_count > 0 else "✅  No duplicates"
            print(f"  Key: {dup_key}  →  {flag}")
        except Exception as e:
            print(f"  ⚠️  Duplicate check failed: {str(e)[:100]}")
    else:
        print(f"  ℹ️  Best grain has < 2 columns — skipping duplicate check")

    # ── Step 9: Enterprise Readiness Score (6 dimensions) ────────────────────
    print(f"\n  Step 9 — Enterprise Readiness Score")

    # Dimension 1: Completeness (20 pts)
    mandatory_fails = sum(
        1 for r in res["dq_results"]
        if r["rule_type"] == "mandatory" and not r["passed"]
    )
    mandatory_total = sum(
        1 for r in res["dq_results"]
        if r["rule_type"] == "mandatory"
    )
    sc_completeness = max(0, 20 - mandatory_fails * 5) if mandatory_total > 0 else 15

    # Dimension 2: Consistency (20 pts)
    consistency_rules = [r for r in res["dq_results"] if r["rule_type"] != "mandatory"]
    if consistency_rules:
        pass_rate = sum(1 for r in consistency_rules if r["passed"]) / len(consistency_rules)
        sc_consistency = round(pass_rate * 20)
    else:
        sc_consistency = 10

    # Dimension 3: Joinability (25 pts)
    bk_cols_present = [c for c in res["col_inventory"] if c["column"].lower() in BUSINESS_KEY_COLS]
    if bk_cols_present:
        avg_null = sum(c["null_pct"] for c in bk_cols_present) / len(bk_cols_present)
        sc_joinability = max(0, round(25 - avg_null / 4))
    else:
        sc_joinability = 5

    # Dimension 4: Temporal Coverage (15 pts)
    if res["date_col"]:
        sc_temporal = 15 if res["temporal_gaps"] == 0 else max(5, 15 - res["temporal_gaps"] * 2)
    else:
        sc_temporal = 0

    # Dimension 5: Business Documentation (10 pts)
    total_cols_cnt = len(res["col_inventory"])
    documented_cnt = sum(1 for c in res["col_inventory"] if c["in_glossary"])
    sc_documentation = round((documented_cnt / total_cols_cnt) * 10) if total_cols_cnt > 0 else 0

    # Dimension 6: Grain Clarity (10 pts)
    if best_grain:
        uniq = best_grain["uniqueness_pct"]
        sc_grain = 10 if uniq == 100 else (8 if uniq >= 95 else (6 if uniq >= 80 else 3))
    else:
        sc_grain = 0

    enterprise_score = (sc_completeness + sc_consistency + sc_joinability +
                        sc_temporal + sc_documentation + sc_grain)

    res.update({
        "score_completeness":    sc_completeness,
        "score_consistency":     sc_consistency,
        "score_joinability":     sc_joinability,
        "score_temporal":        sc_temporal,
        "score_documentation":   sc_documentation,
        "score_grain":           sc_grain,
        "enterprise_score":      enterprise_score,
    })

    def score_label(s):
        return "🟢 ENTERPRISE READY" if s >= 80 else ("🟡 CONDITIONAL" if s >= 60 else "🔴 NOT READY")

    print(f"  {'Dimension':<30} {'Score':>8} {'Max':>5}")
    print("  " + "─" * 45)
    print(f"  {'Completeness':<30} {sc_completeness:>8} {20:>5}")
    print(f"  {'Consistency (DQ Rules)':<30} {sc_consistency:>8} {20:>5}")
    print(f"  {'Joinability (estimated)':<30} {sc_joinability:>8} {25:>5}")
    print(f"  {'Temporal Coverage':<30} {sc_temporal:>8} {15:>5}")
    print(f"  {'Business Documentation':<30} {sc_documentation:>8} {10:>5}")
    print(f"  {'Grain Clarity':<30} {sc_grain:>8} {10:>5}")
    print("  " + "─" * 45)
    print(f"  {'ENTERPRISE SCORE':<30} {enterprise_score:>8} {100:>5}  {score_label(enterprise_score)}")

    results[source_key] = res
    df.unpersist()

    # ── Per-source execution log (one record per source) ─────────────────────
    _src_status  = "ERROR" if res["status"] == "ERROR" else (
        "PARTIAL" if res["errors"] else "SUCCESS"
    )
    _src_errors  = [
        make_error(
            step      = "Discovery loop",
            category  = "SCHEMA_MISMATCH",
            severity  = "HIGH",
            message   = err_msg,
        )
        for err_msg in res["errors"]
    ]
    write_execution_log(
        notebook_id  = "00b_snowflake_discovery",
        source_name  = source_key,
        status       = _src_status,
        duration_sec = _time.time() - _NB_START,
        errors       = _src_errors,
        warnings     = [
            f"Declared business key not found: {m}"
            for m in [e for e in res.get("errors", []) if "business key" in e.lower()]
        ],
        metrics      = {
            "total_rows":        res["total_rows"],
            "total_cols":        res["total_cols"],
            "date_col":          res.get("date_col"),
            "date_min":          res.get("date_min"),
            "date_max":          res.get("date_max"),
            "date_distinct":     res.get("date_distinct"),
            "temporal_gaps":     res.get("temporal_gaps", 0),
            "dup_count":         res.get("dup_count", 0),
            "dup_pct":           res.get("dup_pct", 0.0),
            "dq_pass_count":     res.get("dq_pass_count", 0),
            "dq_fail_count":     res.get("dq_fail_count", 0),
            "score_completeness": res.get("score_completeness", 0),
            "score_consistency":  res.get("score_consistency", 0),
            "score_joinability":  res.get("score_joinability", 0),
            "score_temporal":     res.get("score_temporal", 0),
            "score_documentation": res.get("score_documentation", 0),
            "score_grain":        res.get("score_grain", 0),
            "enterprise_score":   res.get("enterprise_score", 0),
        },
        output_files = [f"{OUTPUT_DIR}/_discovery_results_cache.json"],
        environment  = ENVIRONMENT,
    )

# COMMAND ----------

# MAGIC %md ## B5 · Summary Scorecard

# COMMAND ----------

print("\n" + "═" * 80)
print("ENTERPRISE READINESS SUMMARY")
print("═" * 80)
print(f"  {'Source':<18} {'Domain':<26} {'Rows':>10}  {'Complete':>9} {'Consist':>8} "
      f"{'Joinabl':>8} {'Temporal':>9} {'DocScore':>9} {'Grain':>7} {'TOTAL':>7}  Status")
print("  " + "─" * 115)

for key, res in results.items():
    status = "❌ ERROR" if res["status"] == "ERROR" else (
        "🟢 READY" if res["enterprise_score"] >= 80
        else ("🟡 COND." if res["enterprise_score"] >= 60 else "🔴 NOT READY")
    )
    print(f"  {key:<18} {res['label']:<26} {res['total_rows']:>10,}  "
          f"{res['score_completeness']:>9} {res['score_consistency']:>8} "
          f"{res['score_joinability']:>8} {res['score_temporal']:>9} "
          f"{res['score_documentation']:>9} {res['score_grain']:>7} "
          f"{res['enterprise_score']:>6}/100  {status}")

for key in undefined:
    print(f"  {key:<18} {DOMAIN_LABELS.get(key,key):<26} {'':>10}  {'':>9} {'':>8} {'':>8} {'':>9} {'':>9} {'':>7}  ⏳ TBD")

print(f"\n  Run notebook 00c_enterprise_catalog_writer.py to generate all 10 CSV outputs")
print(f"  and persist results to Snowflake MDP_ANALYTICS.METADATA schema.")

# COMMAND ----------

# MAGIC %md ## B6 · Pass Results to 00c (via notebook exit value or shared storage)

# COMMAND ----------

# The `results` dict is available as a module-level variable.
# When running in sequence via %run, 00c will access it directly.
# If running independently, serialize to a temp Delta table or JSON file.

import json

_results_serializable = {}
for k, v in results.items():
    _r = dict(v)
    # Replace un-serializable types
    _r["grain_candidates"] = v.get("grain_candidates", [])
    _results_serializable[k] = _r

try:
    _out_path = f"{OUTPUT_DIR}/_discovery_results_cache.json"
    with open(_out_path, "w", encoding="utf-8") as _f:
        json.dump(_results_serializable, _f, default=str, indent=2)
    print(f"✅ Discovery results cached to: {_out_path}")
    print(f"   → Proceed to run: 00c_enterprise_catalog_writer.py")
except Exception as _e:
    print(f"⚠️  Could not serialize results cache: {_e}")
    print(f"   → Run 00c in the same session immediately after this notebook.")

# COMMAND ----------

# MAGIC %md ## B7 · Aggregate Execution Log (Run Summary)

# COMMAND ----------
# One additional summary record covering the full notebook run
_summary_errors_count = sum(1 for r in results.values() if r.get("status") == "ERROR")
_summary_ok_count     = len(results) - _summary_errors_count
write_execution_log(
    notebook_id  = "00b_snowflake_discovery",
    source_name  = "__ALL_SOURCES__",
    status       = "ERROR" if _summary_errors_count == len(results) else (
                   "PARTIAL" if _summary_errors_count > 0 else "SUCCESS"),
    duration_sec = _time.time() - _NB_START,
    errors       = [],
    warnings     = ([f"{len(undefined)} source(s) still None in SOURCES config: {list(undefined.keys())}"]
                    if undefined else []),
    metrics      = {
        "sources_defined":  len(defined),
        "sources_pending":  len(undefined),
        "sources_ok":       _summary_ok_count,
        "sources_errored":  _summary_errors_count,
        "enterprise_scores": {
            k: r.get("enterprise_score", 0) for k, r in results.items()
        },
    },
    output_files = [f"{OUTPUT_DIR}/_discovery_results_cache.json"],
    environment  = ENVIRONMENT,
)
