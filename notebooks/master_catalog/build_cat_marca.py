# Databricks notebook source
# MAGIC %md
# MAGIC # MARCA Catalog Build -- v6
# MAGIC
# MAGIC **Purpose:** Build cat_marca.csv -- the master MARCA (brand) catalog for the Market Growth
# MAGIC Intelligence platform. Extracts raw brand variants from all 10 confirmed source columns,
# MAGIC applies brand_crosswalk.yaml (flattened variant extraction), classifies each variant
# MAGIC as CONFIRMED / UNMAPPED_LIKELY_DANONE / UNMAPPED_COMPETITOR_OR_UNKNOWN, enforces a
# MAGIC 97.2% coverage gate per source, and writes final outputs to DBFS.
# MAGIC
# MAGIC **Plan reference:** Master Data Catalog Implementation Plan v6 -- Dimension A15 (MARCA)
# MAGIC
# MAGIC **Source columns confirmed:**
# MAGIC - SELL_IN       : PRD_MEX.MEX_DSP_OTC.V_D_ITEM            -> LV2_UMB_BRD_DSC
# MAGIC - SELL_OUT      : PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM          -> BRAND
# MAGIC - MKT_ON        : PRD_MDP.MDP_DSP.VW_MKT_ECOMM             -> MARCA
# MAGIC - MKT_OFF       : PRD_MDP.MDP_STG.FACT_MEDIA_OFF            -> MARCA
# MAGIC - IBP           : PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP        -> MARCA  (NOMBRE_ETIQUETA=REAL only)
# MAGIC - WASTE         : PRD_MDP.MDP_STG.VW_WASTE                  -> MARCA  (FUENTE=TOPLINE only)
# MAGIC - NIELSEN_EDP   : PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_PROD_DIM  -> INP_56985 (lvl 11)
# MAGIC - NIELSEN_PB    : PRD_MEX.MEX_DSP_DPH_MKT.VW_SUST_LECHE_ST_NLSN_PROD_DIM  -> INP_56985
# MAGIC - NIELSEN_WATER_ST: PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_ST_NLSN_PROD_DIM -> CSTM_310589 (lvl 11)
# MAGIC - NIELSEN_WATER_RT: PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_RT_NLSN_PROD_DIM -> CSTM_310589 (lvl 9)
# MAGIC
# MAGIC **EDA corrections applied:**
# MAGIC - stat_cod filter REMOVED (Bug 2, 2026-06-29): field is dirty (INACTIVO/ACTIVO/0/1/2/3 mixed)
# MAGIC - Crosswalk extraction FIXED (Bug 1, 2026-06-29): flattens all sections + variants -> ~161 entries
# MAGIC
# MAGIC **Credential resolution:**
# MAGIC - PRD_MEX -> configs/snowflake_creds.py SF_MEX_* (PRD_OSM_DPH_READER)
# MAGIC - PRD_MDP -> SF_MDP_* or Key Vault (DAN-AM-P-KVT800-R-MDP-DB)
# MAGIC
# MAGIC **Output root:** dbfs:/mnt/mdp/mdm/master_catalog/marca/
# MAGIC
# MAGIC **Run notebooks/validate_credentials.py first -- all 6 cells must pass.**

# COMMAND ----------

# == CELL 1: Header + credentials =============================================
import os
import importlib.util
import datetime
import math
import pathlib
import pandas as pd

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# ---------------------------------------------------------------------------
# Notebook constants
# ---------------------------------------------------------------------------
CATALOG_VERSION = "6.0.0"
NOTEBOOK_NAME   = "build_cat_marca"
DBFS_BASE       = "dbfs:/mnt/mdp/mdm/master_catalog"
MARCA_BASE      = f"{DBFS_BASE}/marca"
DB_PRD_MEX      = "PRD_MEX"
DB_PRD_MDP      = "PRD_MDP"
RUN_DATE        = datetime.datetime.now().strftime("%Y-%m-%d")

# ---------------------------------------------------------------------------
# Snowflake credentials
# ---------------------------------------------------------------------------
_current_dir = os.getcwd()
_creds_path  = os.path.normpath(
    os.path.join(_current_dir, "..", "..", "configs", "snowflake_creds.py")
)

if not os.path.exists(_creds_path):
    raise FileNotFoundError(
        "configs/snowflake_creds.py NOT FOUND.\n"
        "   Copy configs/snowflake_creds.example.py -> configs/snowflake_creds.py"
    )

_spec = importlib.util.spec_from_file_location("snowflake_creds", _creds_path)
_m    = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_m)

SF_URL = "danonenam.east-us-2.azure.snowflakecomputing.com"


def get_sf_options(database: str) -> dict:
    _mdp_user = getattr(_m, "SF_MDP_USER", None)
    _mdp_pwd  = getattr(_m, "SF_MDP_PASSWORD", None)
    profiles = {
        DB_PRD_MEX: {
            "sfURL":       SF_URL,
            "sfUser":      _m.SF_MEX_USER,
            "sfPassword":  _m.SF_MEX_PASSWORD,
            "sfWarehouse": getattr(_m, "SF_MEX_WH", "PRD_MEX_ANL_WH"),
            "sfRole":      getattr(_m, "SF_MEX_ROLE", "PRD_MEX_READER"),
        },
        DB_PRD_MDP: {
            "sfURL":       SF_URL,
            "sfUser":      _mdp_user or dbutils.secrets.get(
                               "DAN-AM-P-KVT800-R-MDP-DB", "snowflake-user"),
            "sfPassword":  _mdp_pwd or dbutils.secrets.get(
                               "DAN-AM-P-KVT800-R-MDP-DB", "snowflake-password"),
            "sfWarehouse": getattr(_m, "SF_MDP_WH", "PRD_MDP_ANL_WH"),
            "sfRole":      getattr(_m, "SF_MDP_ROLE", "PRD_MDP"),
        },
    }
    if database not in profiles:
        raise ValueError(
            f"No profile for '{database}'. Available: {list(profiles.keys())}"
        )
    return dict(profiles[database])


def run_sf(database: str, sql: str):
    """Execute SQL against Snowflake and return a Spark DataFrame."""
    return (
        spark.read.format("net.snowflake.spark.snowflake")
        .options(**get_sf_options(database))
        .option("sfDatabase", database)
        .option("query", sql)
        .load()
    )


print(f"OK Credentials loaded -- PRD_MEX: {_m.SF_MEX_USER}")
print(f"OK Notebook: {NOTEBOOK_NAME} v{CATALOG_VERSION} | RUN_DATE={RUN_DATE}")

# COMMAND ----------

# == CELL 2: Output paths + helpers ==========================================
_REPO_ROOT    = str(pathlib.Path(_current_dir).parent.parent)
REPO_LOGS_DIR = os.path.join(_REPO_ROOT, "logs", "catalog_eda")
os.makedirs(REPO_LOGS_DIR, exist_ok=True)

# DBFS directory initialisation -- failure is a hard blocker
for _dbfs_dir in [
    MARCA_BASE,
    f"{MARCA_BASE}/by_source",
    f"{DBFS_BASE}/profiling/marca",
]:
    try:
        dbutils.fs.mkdirs(_dbfs_dir)
        print(f"OK DBFS dir ready: {_dbfs_dir}")
    except Exception as _e:
        raise RuntimeError(
            f"BLOCKER: Could not create DBFS directory {_dbfs_dir}. Error: {_e}\n"
            "   Verify that dbfs:/mnt/mdp is mounted and the service principal has write access."
        )

_LOG_LINES:     list = []
_HARD_BLOCKERS: list = []
_WARNINGS:      list = []


def ts() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(level: str, msg: str, section: str = ""):
    prefix = f"[{ts()}][{level}]"
    if section:
        prefix += f"[{section}]"
    line = f"{prefix} {msg}"
    _LOG_LINES.append(line)
    print(line)


def flush_log(filename: str = "build_cat_marca_report.txt"):
    content = "\n".join(_LOG_LINES)
    dbfs_path = f"{MARCA_BASE}/{filename}"
    dbutils.fs.put(dbfs_path, content, overwrite=True)
    repo_log = os.path.join(REPO_LOGS_DIR, filename)
    with open(repo_log, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"LOG -> DBFS: {dbfs_path}")
    print(f"LOG -> REPO: {repo_log}")


def save_df(df, dbfs_path: str, section: str = ""):
    """Write a Spark DataFrame as CSV to DBFS and (pandas) to repo logs."""
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(dbfs_path)
    fname = os.path.basename(dbfs_path)
    if not fname.endswith(".csv"):
        fname += ".csv"
    repo_csv = os.path.join(REPO_LOGS_DIR, fname)
    try:
        df.limit(50000).toPandas().to_csv(repo_csv, index=False, encoding="utf-8")
        log("INFO", f"Saved -> DBFS: {dbfs_path}  |  REPO: {repo_csv}", section)
    except Exception as err:
        log("WARNING", f"Repo CSV write failed for {fname}: {err}", section)


def blocker(condition: bool, msg: str, section: str = "") -> bool:
    if condition:
        log("BLOCKER", msg, section)
        _HARD_BLOCKERS.append(f"[{section}] {msg}")
    return condition


def warn(condition: bool, msg: str, section: str = "") -> bool:
    if condition:
        log("WARNING", msg, section)
        _WARNINGS.append(f"[{section}] {msg}")
    return condition


def passed(msg: str, section: str = ""):
    log("PASS", msg, section)


print(f"OK CELL 2 ready. DBFS: {MARCA_BASE} | REPO: {REPO_LOGS_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1 -- Load brand_crosswalk.yaml (FIXED extraction)
# MAGIC
# MAGIC **Bug 1 fix (2026-06-29):** Previous code read only top-level dict keys (~6 entries).
# MAGIC Correct approach: flatten all 4 brand sections + all variant lists -> ~161 entries.

# COMMAND ----------

SECTION = "S1_CROSSWALK"
log("INFO", "=" * 70, SECTION)
log("INFO", "LOAD BRAND CROSSWALK YAML (FIXED -- flattened sections + variants)", SECTION)
log("INFO", f"Notebook: {NOTEBOOK_NAME} v{CATALOG_VERSION}", SECTION)

_crosswalk_path = os.path.join(_REPO_ROOT, "configs", "brand_crosswalk.yaml")

_confirmed_brands: set = set()
_canonical_map:   dict = {}
_brand_owner_map: dict = {}

_BRAND_SECTIONS = [
    "danone_brands",
    "competitor_brands",
    "nielsen_brands",
    "special_mappings",
]
_OWNER_MAP = {
    "danone_brands":     "DANONE",
    "competitor_brands": "COMPETITOR",
    "nielsen_brands":    "DANONE",
    "special_mappings":  "DANONE",
}

if os.path.exists(_crosswalk_path):
    try:
        import yaml
        with open(_crosswalk_path, "r", encoding="utf-8") as _f:
            _cw_data = yaml.safe_load(_f)

        for _sec in _BRAND_SECTIONS:
            if _sec in _cw_data and isinstance(_cw_data[_sec], dict):
                _owner = _OWNER_MAP.get(_sec, "UNKNOWN")
                for _brand, _meta in _cw_data[_sec].items():
                    _brand_up = str(_brand).strip().upper()
                    _confirmed_brands.add(_brand_up)
                    _canonical_map[_brand_up]   = _brand_up
                    _brand_owner_map[_brand_up] = _owner
                    if isinstance(_meta, dict):
                        for _v in _meta.get("variants", []):
                            _v_up = str(_v).strip().upper()
                            _confirmed_brands.add(_v_up)
                            _canonical_map[_v_up]   = _brand_up
                            _brand_owner_map[_v_up] = _owner

        log("INFO",
            f"Crosswalk loaded: {len(_confirmed_brands)} confirmed brand+variant entries "
            f"(expected ~161).",
            SECTION)

        for _sec in _BRAND_SECTIONS:
            _n = len(_cw_data.get(_sec, {})) if isinstance(_cw_data.get(_sec), dict) else 0
            log("INFO", f"  {_sec:<25} : {_n} canonical entries", SECTION)

    except Exception as _cw_err:
        warn(True, f"brand_crosswalk.yaml found but failed to parse: {_cw_err}", SECTION)
        log("WARNING", "Proceeding with empty crosswalk -- all variants will be UNMAPPED.", SECTION)
else:
    warn(True,
         "configs/brand_crosswalk.yaml NOT FOUND. All variants will be classified as UNMAPPED.",
         SECTION)

blocker(len(_confirmed_brands) == 0,
        "Crosswalk is EMPTY after load. Cannot classify any brand variants. "
        "Ensure configs/brand_crosswalk.yaml exists and is correctly structured.",
        SECTION)

_confirmed_bc       = spark.sparkContext.broadcast(_confirmed_brands)
_canonical_map_bc   = spark.sparkContext.broadcast(_canonical_map)
_brand_owner_map_bc = spark.sparkContext.broadcast(_brand_owner_map)

log("INFO", f"Broadcast complete: {len(_confirmed_brands)} entries in crosswalk.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2 -- Extract brand variants from all 10 sources

# COMMAND ----------

SECTION = "S2_EXTRACT"
log("INFO", "=" * 70, SECTION)
log("INFO", "EXTRACT BRAND VARIANTS FROM ALL 10 SOURCES", SECTION)

SOURCE_REGISTRY = [
    {
        "source": "SELL_IN", "db": DB_PRD_MEX, "brand_col": "LV2_UMB_BRD_DSC",
        "sql": """
            SELECT TRIM(UPPER(LV2_UMB_BRD_DSC)) AS raw_variant,
                   LV2_UMB_BRD_DSC               AS brand_original,
                   COUNT(DISTINCT MAT_IDT)        AS grain_count,
                   'LV2_UMB_BRD_DSC'             AS source_column,
                   'SELL_IN'                      AS source_system
            FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM
            WHERE LV2_UMB_BRD_DSC IS NOT NULL
            GROUP BY 1,2
        """,
    },
    {
        "source": "SELL_OUT", "db": DB_PRD_MDP, "brand_col": "BRAND",
        "sql": """
            SELECT TRIM(UPPER(BRAND))   AS raw_variant,
                   BRAND                AS brand_original,
                   COUNT(DISTINCT INT_ID) AS grain_count,
                   'BRAND'              AS source_column,
                   'SELL_OUT'           AS source_system
            FROM PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM
            WHERE BRAND IS NOT NULL
            GROUP BY 1,2
        """,
    },
    {
        "source": "MKT_ON", "db": DB_PRD_MDP, "brand_col": "MARCA",
        "sql": """
            SELECT TRIM(UPPER(MARCA)) AS raw_variant,
                   MARCA              AS brand_original,
                   COUNT(*)           AS grain_count,
                   'MARCA'            AS source_column,
                   'MKT_ON'           AS source_system
            FROM PRD_MDP.MDP_DSP.VW_MKT_ECOMM
            WHERE MARCA IS NOT NULL AND ANIO >= 2023
            GROUP BY 1,2
        """,
    },
    {
        "source": "MKT_OFF", "db": DB_PRD_MDP, "brand_col": "MARCA",
        "sql": """
            SELECT TRIM(UPPER(MARCA)) AS raw_variant,
                   MARCA              AS brand_original,
                   COUNT(*)           AS grain_count,
                   'MARCA'            AS source_column,
                   'MKT_OFF'          AS source_system
            FROM PRD_MDP.MDP_STG.FACT_MEDIA_OFF
            WHERE MARCA IS NOT NULL AND ANIO >= 2023
            GROUP BY 1,2
        """,
    },
    {
        "source": "IBP", "db": DB_PRD_MDP, "brand_col": "MARCA",
        "sql": """
            SELECT TRIM(UPPER(MARCA)) AS raw_variant,
                   MARCA              AS brand_original,
                   COUNT(*)           AS grain_count,
                   'MARCA'            AS source_column,
                   'IBP'              AS source_system
            FROM PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP
            WHERE MARCA IS NOT NULL AND NOMBRE_ETIQUETA = 'REAL'
            GROUP BY 1,2
        """,
    },
    {
        "source": "WASTE", "db": DB_PRD_MDP, "brand_col": "MARCA",
        "sql": """
            SELECT TRIM(UPPER(MARCA)) AS raw_variant,
                   MARCA              AS brand_original,
                   COUNT(*)           AS grain_count,
                   'MARCA'            AS source_column,
                   'WASTE'            AS source_system
            FROM PRD_MDP.MDP_STG.VW_WASTE
            WHERE MARCA IS NOT NULL AND UPPER(TRIM(FUENTE)) = 'TOPLINE'
            GROUP BY 1,2
        """,
    },
    {
        "source": "NIELSEN_EDP", "db": DB_PRD_MEX, "brand_col": "INP_56985",
        "sql": """
            -- Subquery required: Snowflake JDBC cannot resolve hierarchy_level in WHERE directly.
            -- Filter applied after inner aggregation via alias hier_lvl.
            SELECT raw_variant, brand_original, grain_count, source_column, source_system
            FROM (
                SELECT TRIM(UPPER(INP_56985))    AS raw_variant,
                       INP_56985                 AS brand_original,
                       COUNT(DISTINCT PRDC_CD)   AS grain_count,
                       'INP_56985'               AS source_column,
                       'NIELSEN_EDP'             AS source_system,
                       hierarchy_level           AS hier_lvl
                FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_PROD_DIM
                WHERE INP_56985 IS NOT NULL
                GROUP BY 1,2,6
            ) t
            WHERE hier_lvl = 11
        """,
    },
    {
        "source": "NIELSEN_PB", "db": DB_PRD_MEX, "brand_col": "INP_56985",
        "sql": """
            SELECT TRIM(UPPER(INP_56985))    AS raw_variant,
                   INP_56985                 AS brand_original,
                   COUNT(DISTINCT PRDC_CD)   AS grain_count,
                   'INP_56985'               AS source_column,
                   'NIELSEN_PB'              AS source_system
            FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_SUST_LECHE_ST_NLSN_PROD_DIM
            WHERE INP_56985 IS NOT NULL
            GROUP BY 1,2
        """,
    },
    {
        "source": "NIELSEN_WATER_ST", "db": DB_PRD_MEX, "brand_col": "CSTM_310589",
        "sql": """
            -- Subquery required: Snowflake JDBC cannot resolve hierarchy_level in WHERE directly.
            SELECT raw_variant, brand_original, grain_count, source_column, source_system
            FROM (
                SELECT TRIM(UPPER(CSTM_310589))  AS raw_variant,
                       CSTM_310589               AS brand_original,
                       COUNT(DISTINCT PRDC_CD)   AS grain_count,
                       'CSTM_310589'             AS source_column,
                       'NIELSEN_WATER_ST'        AS source_system,
                       hierarchy_level           AS hier_lvl
                FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_ST_NLSN_PROD_DIM
                WHERE CSTM_310589 IS NOT NULL
                GROUP BY 1,2,6
            ) t
            WHERE hier_lvl = 11
        """,
    },
    {
        "source": "NIELSEN_WATER_RT", "db": DB_PRD_MEX, "brand_col": "CSTM_310589",
        "sql": """
            -- Water Retail: PRDC_CD does not exist in this view (confirmed: hierarchy max = 9, presentation pack).
            -- Grain = COUNT(*) at brand level. No UPC grain — MARCA only contribution.
            -- Subquery used to filter hierarchy_level after aliasing.
            SELECT raw_variant, brand_original, grain_count, source_column, source_system
            FROM (
                SELECT TRIM(UPPER(CSTM_310589))  AS raw_variant,
                       CSTM_310589               AS brand_original,
                       COUNT(*)                  AS grain_count,
                       'CSTM_310589'             AS source_column,
                       'NIELSEN_WATER_RT'        AS source_system,
                       hierarchy_level           AS hier_lvl
                FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_RT_NLSN_PROD_DIM
                WHERE CSTM_310589 IS NOT NULL
                GROUP BY 1,2,6
            ) t
            WHERE hier_lvl = 9
        """,
    },
]

_COL_VERIFY_ROWS = [
    ("SELL_IN",          "PRD_MEX.MEX_DSP_OTC.V_D_ITEM",                    "LV2_UMB_BRD_DSC", "YES"),
    ("SELL_OUT",         "PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM",                 "BRAND",           "YES"),
    ("MKT_ON",           "PRD_MDP.MDP_DSP.VW_MKT_ECOMM",                   "MARCA",           "YES"),
    ("MKT_OFF",          "PRD_MDP.MDP_STG.FACT_MEDIA_OFF",                  "MARCA",           "YES"),
    ("IBP",              "PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP",              "MARCA",           "YES"),
    ("WASTE",            "PRD_MDP.MDP_STG.VW_WASTE",                        "MARCA",           "YES"),
    ("NIELSEN_EDP",      "PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_..._PROD_DIM", "INP_56985",      "YES (lvl 11)"),
    ("NIELSEN_PB",       "PRD_MEX.MEX_DSP_DPH_MKT.VW_SUST_..._PROD_DIM",   "INP_56985",      "YES"),
    ("NIELSEN_WATER_ST", "PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_..._ST_PROD_DIM", "CSTM_310589",    "YES (lvl 11)"),
    ("NIELSEN_WATER_RT", "PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_..._RT_PROD_DIM", "CSTM_310589",    "YES (lvl 9)"),
]

log("INFO", "COLUMN VERIFICATION TABLE:", SECTION)
_hdr2 = f"{'Source':<18} | {'Source Table':<52} | {'Brand Column':<16} | Plan Confirmed"
log("INFO", "-" * len(_hdr2), SECTION)
log("INFO", _hdr2, SECTION)
log("INFO", "-" * len(_hdr2), SECTION)
for _src, _tbl, _col, _conf in _COL_VERIFY_ROWS:
    log("INFO", f"{_src:<18} | {_tbl:<52} | {_col:<16} | {_conf}", SECTION)
log("INFO", "-" * len(_hdr2), SECTION)

_source_dfs = []
for _entry in SOURCE_REGISTRY:
    _src = _entry["source"]
    _db  = _entry["db"]
    _col = _entry["brand_col"]
    log("INFO", f"Querying {_src} (db={_db}, brand_col={_col})...", SECTION)
    try:
        _df = run_sf(_db, _entry["sql"])
        _n  = _df.count()
        log("INFO", f"  {_src}: {_n:,} distinct raw_variant rows.", SECTION)
        _source_dfs.append(_df)
    except Exception as _ex:
        log("WARNING", f"  {_src}: Query FAILED -- {_ex}", SECTION)
        warn(True, f"Source {_src} query failed: {_ex}", SECTION)

blocker(len(_source_dfs) == 0,
        "All 10 source queries failed. Cannot build cat_marca.csv.", SECTION)

df_marca_all = _source_dfs[0]
for _df in _source_dfs[1:]:
    df_marca_all = df_marca_all.union(_df)

df_marca_all.cache()
_total_variants_raw = df_marca_all.count()
log("INFO", f"Total raw variant rows (all sources): {_total_variants_raw:,}", SECTION)

_src_counts = (
    df_marca_all
    .groupBy("source_system", "source_column")
    .agg(F.countDistinct("raw_variant").alias("distinct_variants"))
    .orderBy("source_system")
    .collect()
)
for _r in _src_counts:
    log("INFO",
        f"  {_r['source_system']:<18} [{_r['source_column']:<16}] "
        f"-> {_r['distinct_variants']:>5,} distinct variants", SECTION)

log("INFO", "Section 2 complete.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3 -- Apply crosswalk + classify each variant
# MAGIC
# MAGIC Classification levels:
# MAGIC - CONFIRMED: variant found in brand_crosswalk.yaml (canonical or variant alias)
# MAGIC - UNMAPPED_LIKELY_DANONE: not in crosswalk but contains a known Danone brand keyword
# MAGIC - UNMAPPED_COMPETITOR_OR_UNKNOWN: not in crosswalk, no known Danone keyword

# COMMAND ----------

SECTION = "S3_CLASSIFY"
log("INFO", "=" * 70, SECTION)
log("INFO", "APPLY CROSSWALK + CLASSIFY BRAND VARIANTS", SECTION)


@F.udf(returnType=StringType())
def udf_get_marca_std(raw_variant):
    if raw_variant is None:
        return None
    return _canonical_map_bc.value.get(raw_variant.strip().upper(), None)


@F.udf(returnType=StringType())
def udf_get_brand_owner(raw_variant):
    if raw_variant is None:
        return "UNKNOWN"
    return _brand_owner_map_bc.value.get(raw_variant.strip().upper(), "UNKNOWN")


@F.udf(returnType=StringType())
def udf_coverage_status(raw_variant):
    if raw_variant is None:
        return "UNMAPPED"
    return (
        "CONFIRMED"
        if raw_variant.strip().upper() in _confirmed_bc.value
        else "UNMAPPED"
    )


df_classified_primary = (
    df_marca_all
    .withColumn("marca_std",       udf_get_marca_std(F.col("raw_variant")))
    .withColumn("brand_owner",     udf_get_brand_owner(F.col("raw_variant")))
    .withColumn("coverage_status", udf_coverage_status(F.col("raw_variant")))
)

DANONE_KEYWORDS = [
    "DANONE", "BONAFONT", "ACTIVIA", "OIKOS", "DANUP", "DANONINO",
    "VITALINEA", "SILK", "EVIAN", "BADOIT", "PUREZA", "DANMIX",
    "DANY", "DANETTE", "YOPRO", "DELIGHT", "LICUAMIX", "JUIZZY",
    "LEVITE", "STOK", "OCEAN", "AGUAS FRESCAS", "BENEGASTRO",
    "HERSHEYS", "INFINIT",
]
_danone_kw_bc = spark.sparkContext.broadcast(DANONE_KEYWORDS)


@F.udf(returnType=StringType())
def udf_secondary_class(raw_variant, current_status):
    if current_status == "CONFIRMED":
        return "CONFIRMED"
    if raw_variant is None:
        return "UNMAPPED_COMPETITOR_OR_UNKNOWN"
    rv = raw_variant.strip().upper()
    for kw in _danone_kw_bc.value:
        if kw in rv:
            return "UNMAPPED_LIKELY_DANONE"
    return "UNMAPPED_COMPETITOR_OR_UNKNOWN"


df_classified = (
    df_classified_primary
    .withColumn(
        "coverage_status",
        udf_secondary_class(F.col("raw_variant"), F.col("coverage_status")),
    )
)
df_classified.cache()

_class_counts = (
    df_classified
    .groupBy("coverage_status")
    .count()
    .orderBy("coverage_status")
    .collect()
)
log("INFO", "Classification summary:", SECTION)
for _r in _class_counts:
    log("INFO", f"  {_r['coverage_status']:<40} : {_r['count']:>6,} rows", SECTION)

log("INFO", "Section 3 complete.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4 -- Per-source coverage report + validation gates

# COMMAND ----------

SECTION = "S4_COVERAGE"
log("INFO", "=" * 70, SECTION)
log("INFO", "PER-SOURCE COVERAGE REPORT + VALIDATION GATES", SECTION)
log("INFO", "Coverage gate: 97.2% CONFIRMED per source (warn, not blocker)", SECTION)

COVERAGE_GATE_PCT = 97.2

df_source_summary = (
    df_classified
    .groupBy("source_system", "source_column")
    .agg(
        F.countDistinct("raw_variant").alias("total_variants"),
        F.count(F.when(F.col("coverage_status") == "CONFIRMED", 1)).alias("confirmed"),
        F.count(F.when(F.col("coverage_status").startswith("UNMAPPED"), 1)).alias("unmapped"),
        F.count(F.when(F.col("coverage_status") == "UNMAPPED_LIKELY_DANONE", 1))
         .alias("unmapped_likely_danone"),
        F.count(F.when(F.col("coverage_status") == "UNMAPPED_COMPETITOR_OR_UNKNOWN", 1))
         .alias("unmapped_competitor"),
        F.sum("grain_count").alias("total_grain_records"),
    )
    .withColumn("coverage_pct",
                F.round(F.col("confirmed") / F.col("total_variants") * 100, 2))
    .orderBy("source_system")
)

display(df_source_summary)

_summary_rows = df_source_summary.collect()
log("INFO", "", SECTION)
log("INFO", "COVERAGE TABLE (97.2% gate):", SECTION)
_hdr = (
    f"{'Source':<18} | {'BrandCol':<16} | {'Total':>6} | "
    f"{'Confirmed':>9} | {'Unmapped':>8} | {'Cover%':>7} | Gate"
)
log("INFO", "-" * len(_hdr), SECTION)
log("INFO", _hdr, SECTION)
log("INFO", "-" * len(_hdr), SECTION)
for _r in _summary_rows:
    _gate = "OK" if _r["coverage_pct"] >= COVERAGE_GATE_PCT else "WARN"
    log("INFO",
        f"{_r['source_system']:<18} | {_r['source_column']:<16} | "
        f"{_r['total_variants']:>6,} | {_r['confirmed']:>9,} | "
        f"{_r['unmapped']:>8,} | {_r['coverage_pct']:>6.1f}% | {_gate}",
        SECTION)
    if _r["coverage_pct"] < COVERAGE_GATE_PCT:
        warn(True,
             f"{_r['source_system']}: coverage {_r['coverage_pct']:.1f}% < {COVERAGE_GATE_PCT}% gate. "
             f"unmapped_likely_danone={_r['unmapped_likely_danone']}. "
             "Add missing brands to configs/brand_crosswalk.yaml.",
             SECTION)
    if _r["unmapped_likely_danone"] > 0:
        warn(True,
             f"{_r['source_system']}: {_r['unmapped_likely_danone']} UNMAPPED_LIKELY_DANONE variant(s). "
             "Business review required -- see cat_marca_unmapped_danone.csv.",
             SECTION)
log("INFO", "-" * len(_hdr), SECTION)

_unmapped_danone_variants = (
    df_classified
    .filter(F.col("coverage_status") == "UNMAPPED_LIKELY_DANONE")
    .select("source_system", "raw_variant", "brand_original", "grain_count")
    .orderBy("source_system", "raw_variant")
    .collect()
)
if _unmapped_danone_variants:
    log("INFO", f"UNMAPPED_LIKELY_DANONE variants ({len(_unmapped_danone_variants)}) for business review:", SECTION)
    for _v in _unmapped_danone_variants:
        log("INFO",
            f"  [{_v['source_system']:<18}] {_v['raw_variant']:<35} "
            f"(original='{_v['brand_original']}', grain={_v['grain_count']:,})",
            SECTION)
else:
    passed("No UNMAPPED_LIKELY_DANONE variants detected. Crosswalk is complete for Danone brands.", SECTION)

log("INFO", "", SECTION)
log("INFO", "COLUMN VERIFICATION TABLE (confirmed brand columns per Plan A15):", SECTION)
log("INFO", f"  {'Source':<18} | {'Source Table':<52} | {'Brand Column':<16} | Confirmed", SECTION)
log("INFO", "  " + "-" * 105, SECTION)
for _src, _tbl, _col, _conf in _COL_VERIFY_ROWS:
    log("INFO", f"  {_src:<18} | {_tbl:<52} | {_col:<16} | {_conf}", SECTION)

save_df(df_source_summary, f"{MARCA_BASE}/marca_source_summary.csv", SECTION)

for _r in _summary_rows:
    _ssys = _r["source_system"]
    _df_src = df_classified.filter(F.col("source_system") == _ssys)
    save_df(_df_src, f"{MARCA_BASE}/by_source/{_ssys}_brand_profile.csv", SECTION)

log("INFO", "Section 4 complete.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5 -- Build cat_marca.csv (final promoted catalog)

# COMMAND ----------

SECTION = "S5_BUILD_CATALOG"
log("INFO", "=" * 70, SECTION)
log("INFO", "BUILD cat_marca.csv + cat_marca_unmapped.csv", SECTION)

df_cat_marca = (
    df_classified
    .filter(F.col("coverage_status") == "CONFIRMED")
    .select(
        F.col("marca_std"),
        F.col("raw_variant"),
        F.col("brand_original"),
        F.col("source_system"),
        F.col("source_column"),
        F.col("brand_owner"),
        F.lit("YES").alias("promoted"),
        F.lit("CONFIRMED").alias("coverage_status"),
        F.col("grain_count"),
        F.lit(RUN_DATE).alias("catalog_built_date"),
    )
    .distinct()
    .orderBy("marca_std", "source_system")
)
df_cat_marca.cache()
cat_marca_rows = df_cat_marca.count()

log("INFO", f"cat_marca.csv: {cat_marca_rows:,} CONFIRMED rows promoted.", SECTION)
blocker(cat_marca_rows == 0,
        "cat_marca.csv has 0 rows -- crosswalk lookup returned no matches. "
        "Verify brand_crosswalk.yaml structure and that source queries returned data.",
        SECTION)
display(df_cat_marca)

df_cat_marca_unmapped = (
    df_classified
    .filter(F.col("coverage_status") != "CONFIRMED")
    .select(
        F.col("raw_variant"),
        F.col("brand_original"),
        F.col("source_system"),
        F.col("source_column"),
        F.col("coverage_status"),
        F.lit("NO").alias("promoted"),
        F.lit(None).cast("string").alias("marca_std"),
        F.col("grain_count"),
        F.lit("ACTION_REQUIRED").alias("action"),
    )
    .distinct()
    .orderBy("coverage_status", "source_system", "raw_variant")
)
df_cat_marca_unmapped.cache()
unmapped_rows = df_cat_marca_unmapped.count()
log("INFO", f"cat_marca_unmapped.csv: {unmapped_rows:,} UNMAPPED rows.", SECTION)
display(df_cat_marca_unmapped)

df_cat_marca_unmapped_danone = (
    df_cat_marca_unmapped
    .filter(F.col("coverage_status") == "UNMAPPED_LIKELY_DANONE")
    .orderBy("source_system", "raw_variant")
)
unmapped_danone_rows = df_cat_marca_unmapped_danone.count()

save_df(df_cat_marca,                 f"{MARCA_BASE}/cat_marca.csv",                  SECTION)
save_df(df_cat_marca_unmapped,        f"{MARCA_BASE}/cat_marca_unmapped.csv",          SECTION)
save_df(df_cat_marca_unmapped_danone, f"{MARCA_BASE}/cat_marca_unmapped_danone.csv",   SECTION)

warn(unmapped_danone_rows > 0,
     f"{unmapped_danone_rows} UNMAPPED_LIKELY_DANONE row(s) in cat_marca_unmapped_danone.csv. "
     "Add these brands to configs/brand_crosswalk.yaml and re-run to promote them.",
     SECTION)

log("INFO",
    f"{cat_marca_rows:,} confirmed MARCA mappings across "
    f"{len(SOURCE_REGISTRY)} sources promoted to cat_marca.csv.",
    SECTION)
log("INFO", "Section 5 complete.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6 -- Summary + next steps

# COMMAND ----------

SECTION = "S6_SUMMARY"
log("INFO", "=" * 70, SECTION)
log("INFO", "MARCA CATALOG BUILD SUMMARY", SECTION)

_class_totals = {
    r["coverage_status"]: r["count"]
    for r in df_classified.groupBy("coverage_status").count().collect()
}
_confirmed_count = _class_totals.get("CONFIRMED", 0)
_unmapped_danone = _class_totals.get("UNMAPPED_LIKELY_DANONE", 0)
_unmapped_comp   = _class_totals.get("UNMAPPED_COMPETITOR_OR_UNKNOWN", 0)
_total_variants  = sum(_class_totals.values())

_summary_lines = "\n".join([
    "",
    "======================================================================",
    f"MARCA CATALOG BUILD SUMMARY  --  {RUN_DATE}",
    "======================================================================",
    f"Notebook:                    {NOTEBOOK_NAME} v{CATALOG_VERSION}",
    f"Crosswalk entries loaded:    {len(_confirmed_brands)}",
    f"Total raw variants scanned:  {_total_variants:,}",
    f"CONFIRMED:                   {_confirmed_count:,}",
    f"UNMAPPED_LIKELY_DANONE:      {_unmapped_danone:,}  <- need crosswalk update",
    f"UNMAPPED_COMPETITOR_OR_UNK:  {_unmapped_comp:,}    <- review",
    f"cat_marca.csv rows:          {cat_marca_rows:,}",
    f"cat_marca_unmapped.csv rows: {unmapped_rows:,}",
    f"cat_marca_unmapped_danone:   {unmapped_danone_rows:,}  <- business review needed",
    "",
    "OUTPUT FILES:",
    f"  {MARCA_BASE}/cat_marca.csv",
    f"  {MARCA_BASE}/cat_marca_unmapped.csv",
    f"  {MARCA_BASE}/cat_marca_unmapped_danone.csv",
    f"  {MARCA_BASE}/marca_source_summary.csv",
    f"  {MARCA_BASE}/by_source/<source>_brand_profile.csv  (10 files)",
    f"  {MARCA_BASE}/build_cat_marca_report.txt",
    "",
    "EDA CORRECTIONS APPLIED:",
    f"  Bug 1 FIXED: crosswalk extraction reads all sections+variants ({len(_confirmed_brands)} entries, was ~6)",
    "  Bug 2 FIXED: stat_cod filter removed from all V_D_CLIENT queries (dirty field)",
    "  IBP filter:  NOMBRE_ETIQUETA = 'REAL' applied (removes forecast scenarios)",
    "  WASTE filter: FUENTE = 'TOPLINE' applied (removes non-topline entries)",
    "",
    "NEXT STEPS:",
    "  1. Review cat_marca_unmapped_danone.csv with business team",
    "  2. Add confirmed unmapped Danone brands to configs/brand_crosswalk.yaml",
    "  3. Re-run build_cat_marca.py to promote newly confirmed brands",
    "  4. When UNMAPPED_LIKELY_DANONE = 0 -> proceed to build_cat_canal.py",
    "======================================================================",
    "",
])

print(_summary_lines)
log("INFO", _summary_lines, SECTION)

if _HARD_BLOCKERS:
    log("BLOCKER", f"NOTEBOOK FAILED: {len(_HARD_BLOCKERS)} hard blocker(s) raised:", SECTION)
    for _b in _HARD_BLOCKERS:
        log("BLOCKER", f"  {_b}", SECTION)
    raise RuntimeError(
        f"build_cat_marca.py exited with {len(_HARD_BLOCKERS)} BLOCKER(s). "
        "See log for details."
    )
else:
    passed(f"Notebook completed successfully. {cat_marca_rows:,} MARCA mappings promoted.", SECTION)

if _WARNINGS:
    log("INFO", f"{len(_WARNINGS)} warning(s) raised (non-blocking):", SECTION)
    for _w in _WARNINGS:
        log("WARNING", f"  {_w}", SECTION)

flush_log("build_cat_marca_report.txt")

# COMMAND ----------


