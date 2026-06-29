# Databricks notebook source
# MAGIC %md
# MAGIC # Master Catalog Source Profiling -- v6.0.0
# MAGIC
# MAGIC **Purpose:** Profile all source data for MARCA, CANAL, MARKET, and UPC catalogs against live
# MAGIC Snowflake data.  Results directly feed MDM file enrichment (cat_marca.csv, cat_canal.csv,
# MAGIC cat_market.csv, cat_upc.csv).
# MAGIC
# MAGIC **Plan reference:** Master Data Catalog Implementation Plan v6 -- Sections D1-D4, MC-A14
# MAGIC
# MAGIC **Output root:** dbfs:/mnt/mdp/mdm/master_catalog/profiling/
# MAGIC
# MAGIC **Run notebooks/validate_credentials.py first -- all 6 cells must pass.**
# MAGIC
# MAGIC **Credential resolution:**
# MAGIC - PRD_MEX -> configs/snowflake_creds.py  SF_MEX_* (PRD_OSM_DPH_READER)
# MAGIC - PRD_MDP -> SF_MDP_* or Key Vault (DAN-AM-P-KVT800-R-MDP-DB)
# MAGIC
# MAGIC **Sections:**
# MAGIC 1. MARCA Profiling  -- 10 brand sources, crosswalk coverage
# MAGIC 2. CANAL Profiling  -- SELL_IN / SELL_OUT / IBP / WASTE + unified seed
# MAGIC 3. MARKET Profiling -- 4 Nielsen MKT_DIM tables, M1 sign-off overlay
# MAGIC 4. UPC Profiling    -- SAP V_D_ITEM, SELL_OUT product, P0/P1 bridge
# MAGIC 5. Summary          -- JSON + flat CSV for MDM enrichment handoff

# COMMAND ----------

# ============================================================
# CELL 1 -- Credentials, utilities, constants, DBFS init
# ============================================================
import os
import importlib.util
import datetime
import math
import json
import pathlib

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ----------------------------------------------------------
# Locate and load credentials module
# ----------------------------------------------------------
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

# ----------------------------------------------------------
# Constants
# ----------------------------------------------------------
CATALOG_VERSION = "6.0.0"
SF_URL          = "danonenam.east-us-2.azure.snowflakecomputing.com"
DB_PRD_MEX      = "PRD_MEX"
DB_PRD_MDP      = "PRD_MDP"
RUN_DATE        = datetime.datetime.now().strftime("%Y-%m-%d")
DBFS_BASE       = "dbfs:/mnt/mdp/mdm/master_catalog"


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
            "sfUser":      _mdp_user or dbutils.secrets.get("DAN-AM-P-KVT800-R-MDP-DB", "snowflake-user"),
            "sfPassword":  _mdp_pwd  or dbutils.secrets.get("DAN-AM-P-KVT800-R-MDP-DB", "snowflake-password"),
            "sfWarehouse": getattr(_m, "SF_MDP_WH", "PRD_MDP_ANL_WH"),
            "sfRole":      getattr(_m, "SF_MDP_ROLE", "PRD_MDP"),
        },
    }
    if database not in profiles:
        raise ValueError(f"No SF profile for '{database}'. Available: {list(profiles.keys())}")
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


print(f"OK Credentials loaded -- PRD_MEX user: {_m.SF_MEX_USER}")

# ----------------------------------------------------------
# DBFS output directories (D4 -- hard blocker if mount missing)
# ----------------------------------------------------------
_DBFS_DIRS = [
    f"{DBFS_BASE}/profiling/marca/",
    f"{DBFS_BASE}/profiling/canal/",
    f"{DBFS_BASE}/profiling/market/",
    f"{DBFS_BASE}/profiling/upc/",
    f"{DBFS_BASE}/profiling/summary/",
]

for _d in _DBFS_DIRS:
    try:
        dbutils.fs.mkdirs(_d)
        print(f"OK DBFS dir ready: {_d}")
    except Exception as _e:
        raise RuntimeError(
            f"BLOCKER: Could not create DBFS directory {_d}.\n"
            f"   Error: {_e}\n"
            "   Verify dbfs:/mnt/mdp is mounted and the service principal has write access."
        )

# ----------------------------------------------------------
# Repo log directory
# ----------------------------------------------------------
_REPO_ROOT    = str(pathlib.Path(_current_dir).parent.parent)
REPO_LOGS_DIR = os.path.join(_REPO_ROOT, "logs", "catalog_profiling")
os.makedirs(REPO_LOGS_DIR, exist_ok=True)

# ----------------------------------------------------------
# Logging infrastructure
# ----------------------------------------------------------
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


def flush_log(filename: str = "catalog_profiling_report.txt"):
    content = "\n".join(_LOG_LINES)
    dbfs_path = f"{DBFS_BASE}/profiling/summary/{filename}"
    dbutils.fs.put(dbfs_path, content, overwrite=True)
    repo_log = os.path.join(REPO_LOGS_DIR, filename)
    with open(repo_log, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"LOG -> DBFS: {dbfs_path}")
    print(f"LOG -> REPO: {repo_log}")


def save_df(df, dbfs_path: str, section: str = ""):
    """Write a Spark DataFrame as CSV to DBFS and as pandas CSV to repo logs."""
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


def d1_score(null_pct: float, distinct_count: int) -> float:
    """D1 scoring: completeness * log1p(distinct_count)."""
    completeness = max(0.0, 1.0 - null_pct / 100.0)
    return round(completeness * math.log1p(max(0, distinct_count)), 4)


print(f"OK CELL 1 ready.  Version={CATALOG_VERSION}  Run date={RUN_DATE}")
print(f"   DBFS base: {DBFS_BASE}/profiling/")
print(f"   Repo logs: {REPO_LOGS_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1 -- MARCA Profiling
# MAGIC
# MAGIC Profile all 10 brand-source column pairs across SELL_IN, SELL_OUT, MKT_ON, MKT_OFF,
# MAGIC IBP, WASTE, and the four Nielsen PROD_DIM views.
# MAGIC
# MAGIC Outputs:
# MAGIC - marca_all_sources_profile.csv  -- full raw variant profile
# MAGIC - marca_unmapped_variants.csv    -- variants not in brand_crosswalk.yaml
# MAGIC - marca_source_summary.csv       -- per-source coverage table
# MAGIC
# MAGIC Gate: blocker() if any source brand coverage < 97.2 %

# COMMAND ----------

SECTION = "S1_MARCA"
log("INFO", "=" * 70, SECTION)
log("INFO", f"MARCA PROFILING -- {CATALOG_VERSION}", SECTION)
log("INFO", "Querying 10 brand-source column pairs from Snowflake...", SECTION)

# ----------------------------------------------------------
# 1A: Individual source queries
# ----------------------------------------------------------

log("INFO", "[1/10] SELL_IN -- LV2_UMB_BRD_DSC from V_D_ITEM", SECTION)
df_marca_sellin = run_sf(DB_PRD_MEX, """
    SELECT
        TRIM(UPPER(LV2_UMB_BRD_DSC))   AS raw_variant,
        LV2_UMB_BRD_DSC                 AS brand_original,
        COUNT(DISTINCT MAT_IDT)          AS row_count,
        'SELL_IN'                        AS source_system
    FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM
    WHERE LV2_UMB_BRD_DSC IS NOT NULL
    GROUP BY 1, 2
""")

log("INFO", "[2/10] SELL_OUT -- BRAND from VW_D_PRODUCT_RM", SECTION)
df_marca_sellout = run_sf(DB_PRD_MDP, """
    SELECT
        TRIM(UPPER(BRAND))     AS raw_variant,
        BRAND                  AS brand_original,
        COUNT(DISTINCT INT_ID) AS row_count,
        'SELL_OUT'             AS source_system
    FROM PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM
    WHERE BRAND IS NOT NULL
    GROUP BY 1, 2
""")

log("INFO", "[3/10] MKT_ON -- MARCA from VW_MKT_ECOMM (ANIO >= 2024)", SECTION)
df_marca_mkton = run_sf(DB_PRD_MDP, """
    SELECT
        TRIM(UPPER(MARCA)) AS raw_variant,
        MARCA              AS brand_original,
        COUNT(*)           AS row_count,
        'MKT_ON'           AS source_system
    FROM PRD_MDP.MDP_DSP.VW_MKT_ECOMM
    WHERE MARCA IS NOT NULL
      AND ANIO >= 2024
    GROUP BY 1, 2
""")

log("INFO", "[4/10] MKT_OFF -- MARCA from FACT_MEDIA_OFF (ANIO >= 2024)", SECTION)
df_marca_mktoff = run_sf(DB_PRD_MDP, """
    SELECT
        TRIM(UPPER(MARCA)) AS raw_variant,
        MARCA              AS brand_original,
        COUNT(*)           AS row_count,
        'MKT_OFF'          AS source_system
    FROM PRD_MDP.MDP_STG.FACT_MEDIA_OFF
    WHERE MARCA IS NOT NULL
      AND ANIO >= 2024
    GROUP BY 1, 2
""")

log("INFO", "[5/10] IBP -- MARCA from VW_FACT_DANONE_IBP (NOMBRE_ETIQUETA='REAL')", SECTION)
df_marca_ibp = run_sf(DB_PRD_MDP, """
    SELECT
        TRIM(UPPER(MARCA)) AS raw_variant,
        MARCA              AS brand_original,
        COUNT(*)           AS row_count,
        'IBP'              AS source_system
    FROM PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP
    WHERE MARCA IS NOT NULL
      AND NOMBRE_ETIQUETA = 'REAL'
    GROUP BY 1, 2
""")

log("INFO", "[6/10] WASTE -- MARCA from VW_WASTE", SECTION)
df_marca_waste = run_sf(DB_PRD_MDP, """
    SELECT
        TRIM(UPPER(MARCA)) AS raw_variant,
        MARCA              AS brand_original,
        COUNT(*)           AS row_count,
        'WASTE'            AS source_system
    FROM PRD_MDP.MDP_STG.VW_WASTE
    WHERE MARCA IS NOT NULL
    GROUP BY 1, 2
""")

log("INFO", "[7/10] NIELSEN_EDP -- INP_56985 from VW_IR_YOG_GEL_MT_NLSN_PROD_DIM (level 11)", SECTION)
df_marca_edp = run_sf(DB_PRD_MEX, """
    SELECT
        TRIM(UPPER(INP_56985))      AS raw_variant,
        INP_56985                   AS brand_original,
        COUNT(DISTINCT PRDC_CD)     AS row_count,
        'NIELSEN_EDP'               AS source_system
    FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_PROD_DIM
    WHERE INP_56985 IS NOT NULL
      AND "hierarchy_level" = 11
    GROUP BY 1, 2
""")

log("INFO", "[8/10] NIELSEN_PB -- INP_56985 from VW_SUST_LECHE_ST_NLSN_PROD_DIM", SECTION)
df_marca_pb = run_sf(DB_PRD_MEX, """
    SELECT
        TRIM(UPPER(INP_56985))      AS raw_variant,
        INP_56985                   AS brand_original,
        COUNT(DISTINCT PRDC_CD)     AS row_count,
        'NIELSEN_PB'                AS source_system
    FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_SUST_LECHE_ST_NLSN_PROD_DIM
    WHERE INP_56985 IS NOT NULL
    GROUP BY 1, 2
""")

log("INFO", "[9/10] NIELSEN_WATER_ST -- CSTM_310589 from VW_IND_AGUA_BNF_ST_NLSN_PROD_DIM (level 11)", SECTION)
df_marca_water_st = run_sf(DB_PRD_MEX, """
    SELECT
        TRIM(UPPER(CSTM_310589))    AS raw_variant,
        CSTM_310589                 AS brand_original,
        COUNT(DISTINCT PRDC_CD)     AS row_count,
        'NIELSEN_WATER_ST'          AS source_system
    FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_ST_NLSN_PROD_DIM
    WHERE CSTM_310589 IS NOT NULL
      AND "hierarchy_level" = 11
    GROUP BY 1, 2
""")

log("INFO", "[10/10] NIELSEN_WATER_RT -- CSTM_310589 from VW_IND_AGUA_BNF_RT_NLSN_PROD_DIM (level 9)", SECTION)
df_marca_water_rt = run_sf(DB_PRD_MEX, """
    SELECT
        TRIM(UPPER(CSTM_310589))    AS raw_variant,
        CSTM_310589                 AS brand_original,
        COUNT(DISTINCT "product_id")  AS row_count,
        'NIELSEN_WATER_RT'            AS source_system
    FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_RT_NLSN_PROD_DIM
    WHERE CSTM_310589 IS NOT NULL
      AND "hierarchy_level" = 9
    GROUP BY 1, 2
""")

# ----------------------------------------------------------
# 1B: Union all 10 sources
# ----------------------------------------------------------
log("INFO", "Unioning all 10 source DataFrames...", SECTION)
df_marca_all = (
    df_marca_sellin
    .union(df_marca_sellout)
    .union(df_marca_mkton)
    .union(df_marca_mktoff)
    .union(df_marca_ibp)
    .union(df_marca_waste)
    .union(df_marca_edp)
    .union(df_marca_pb)
    .union(df_marca_water_st)
    .union(df_marca_water_rt)
)
df_marca_all.cache()
marca_total_rows = df_marca_all.count()
log("INFO", f"Total rows in unioned MARCA profile: {marca_total_rows:,}", SECTION)

# ----------------------------------------------------------
# 1C: Per-source distinct variant count and total row_count
# ----------------------------------------------------------
log("INFO", "Computing per-source variant counts...", SECTION)
df_marca_per_source = (
    df_marca_all
    .groupBy("source_system")
    .agg(
        F.countDistinct("raw_variant").alias("total_variants_per_source"),
        F.sum("row_count").alias("total_row_count"),
    )
    .orderBy("source_system")
)
df_marca_per_source.cache()
log("INFO", "Per-source variant summary:", SECTION)
df_marca_per_source.show(20, truncate=False)

# ----------------------------------------------------------
# 1D: Load brand crosswalk YAML for coverage classification
# ----------------------------------------------------------
log("INFO", "Loading brand_crosswalk.yaml for coverage classification...", SECTION)
_crosswalk_path = os.path.join(_REPO_ROOT, "configs", "brand_crosswalk.yaml")
_confirmed_brands: set = set()

if os.path.exists(_crosswalk_path):
    try:
        import yaml
        with open(_crosswalk_path, "r", encoding="utf-8") as _f:
            _cw_data = yaml.safe_load(_f)
        # Flatten all canonical names + all raw variants from all brand sections
        _confirmed_brands = set()
        _BRAND_SECTIONS = ["danone_brands", "competitor_brands", "nielsen_brands", "special_mappings"]
        for _sec in _BRAND_SECTIONS:
            if _sec in _cw_data and isinstance(_cw_data[_sec], dict):
                for _brand, _meta in _cw_data[_sec].items():
                    _confirmed_brands.add(str(_brand).strip().upper())
                    if isinstance(_meta, dict):
                        for _v in _meta.get("variants", []):
                            _confirmed_brands.add(str(_v).strip().upper())
        log("INFO", f"Crosswalk loaded: {len(_confirmed_brands)} confirmed brand+variant entries.", SECTION)
        # Expected: ~161 entries (32 Danone + competitors + Nielsen + variants)
    except Exception as _cw_err:
        warn(True, f"brand_crosswalk.yaml found but failed to parse: {_cw_err}", SECTION)
else:
    warn(True, "configs/brand_crosswalk.yaml NOT FOUND. All variants will be classified as UNMAPPED.", SECTION)

# Broadcast confirmed brands set for UDF
_confirmed_bc = spark.sparkContext.broadcast(_confirmed_brands)


def _classify_brand(raw_variant):
    if raw_variant is None:
        return "UNMAPPED"
    return "CONFIRMED" if raw_variant.strip().upper() in _confirmed_bc.value else "UNMAPPED"


_classify_brand_udf = F.udf(_classify_brand)

df_marca_classified = df_marca_all.withColumn(
    "coverage_status", _classify_brand_udf(F.col("raw_variant"))
)
df_marca_classified.cache()

# ----------------------------------------------------------
# 1E: Per-source coverage %
# ----------------------------------------------------------
log("INFO", "Computing per-source coverage percentages...", SECTION)
df_marca_coverage = (
    df_marca_classified
    .groupBy("source_system")
    .agg(
        F.countDistinct("raw_variant").alias("total_variants"),
        F.countDistinct(
            F.when(F.col("coverage_status") == "CONFIRMED", F.col("raw_variant"))
        ).alias("confirmed_variants"),
        F.countDistinct(
            F.when(F.col("coverage_status") == "UNMAPPED", F.col("raw_variant"))
        ).alias("unmapped_variants"),
    )
    .withColumn(
        "coverage_pct",
        F.round(F.col("confirmed_variants") / F.col("total_variants") * 100, 2)
    )
    .orderBy("source_system")
)
df_marca_coverage.cache()

log("INFO", "Per-source coverage table:", SECTION)
df_marca_coverage.show(20, truncate=False)

# Coverage gate: blocker if any source < 97.2%
_coverage_rows = df_marca_coverage.collect()
for _row in _coverage_rows:
    _src   = _row["source_system"]
    _cov   = float(_row["coverage_pct"]) if _row["coverage_pct"] is not None else 0.0
    _total = int(_row["total_variants"])
    _unmap = int(_row["unmapped_variants"])
    log("INFO", f"  {_src:<20} | total={_total:>5} | confirmed={int(_row['confirmed_variants']):>5} | unmapped={_unmap:>5} | coverage={_cov:.2f}%", SECTION)
    blocker(
        _cov < 97.2,
        f"Brand coverage for {_src} = {_cov:.2f}% < 97.2% threshold. "
        f"Unmapped variants: {_unmap}. MDM enrichment blocked.",
        SECTION,
    )

# ----------------------------------------------------------
# 1F: Build output DataFrames
# ----------------------------------------------------------

# Full profile
df_marca_full_profile = df_marca_classified.select(
    "source_system", "raw_variant", "brand_original", "row_count", "coverage_status"
).orderBy("source_system", "raw_variant")

# Unmapped only
df_marca_unmapped = (
    df_marca_classified
    .filter(F.col("coverage_status") == "UNMAPPED")
    .select("source_system", "raw_variant", "brand_original", "row_count")
    .orderBy("source_system", "raw_variant")
)

# Summary: join per-source variant counts with coverage
df_marca_source_summary = df_marca_coverage.join(
    df_marca_per_source.select("source_system", "total_row_count"),
    on="source_system", how="left"
).orderBy("source_system")

# ----------------------------------------------------------
# 1G: Write outputs
# ----------------------------------------------------------
_MARCA_BASE = f"{DBFS_BASE}/profiling/marca"

log("INFO", "Writing marca_all_sources_profile.csv...", SECTION)
save_df(df_marca_full_profile, f"{_MARCA_BASE}/marca_all_sources_profile.csv", SECTION)

log("INFO", "Writing marca_unmapped_variants.csv...", SECTION)
save_df(df_marca_unmapped, f"{_MARCA_BASE}/marca_unmapped_variants.csv", SECTION)

log("INFO", "Writing marca_source_summary.csv...", SECTION)
save_df(df_marca_source_summary, f"{_MARCA_BASE}/marca_source_summary.csv", SECTION)

# ----------------------------------------------------------
# 1H: Display with clear headers
# ----------------------------------------------------------
print("\n" + "=" * 60)
print("MARCA -- ALL SOURCES PROFILE (first 100 rows)")
print("=" * 60)
display(df_marca_full_profile.limit(100))

print("\n" + "=" * 60)
print("MARCA -- UNMAPPED VARIANTS")
print("=" * 60)
display(df_marca_unmapped)

print("\n" + "=" * 60)
print("MARCA -- PER-SOURCE COVERAGE SUMMARY")
print("=" * 60)
display(df_marca_source_summary)

# Capture summary stats for Section 5
_marca_total_raw       = df_marca_all.select("raw_variant").distinct().count()
_marca_confirmed_total = df_marca_classified.filter(F.col("coverage_status") == "CONFIRMED").select("raw_variant").distinct().count()
_marca_unmapped_total  = _marca_total_raw - _marca_confirmed_total
_marca_coverage_pct    = round(_marca_confirmed_total / max(_marca_total_raw, 1) * 100, 2)
_marca_by_source_dict = {
    r["source_system"]: {
        "total_variants": int(r["total_variants"]),
        "confirmed":      int(r["confirmed_variants"]),
        "unmapped":       int(r["unmapped_variants"]),
        "coverage_pct":   float(r["coverage_pct"]) if r["coverage_pct"] is not None else 0.0,
    }
    for r in _coverage_rows
}

log("INFO", f"Section 1 complete. Total raw variants: {_marca_total_raw} | "
            f"Confirmed: {_marca_confirmed_total} | Unmapped: {_marca_unmapped_total} | "
            f"Overall coverage: {_marca_coverage_pct:.2f}%", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2 -- CANAL Profiling
# MAGIC
# MAGIC Profile channel dimensions across all valid CANAL sources:
# MAGIC - 2A: SELL_IN V_D_CLIENT (3 candidate columns + D1 scoring)
# MAGIC - 2B: SELL_OUT VW_D_STORE_RM (3-level hierarchy + geo)
# MAGIC - 2C: IBP VW_FACT_DANONE_IBP (5-level hierarchy)
# MAGIC - 2D: WASTE VW_WASTE CANAL column
# MAGIC - 2E: Unified canal seed (overlap SELL_IN intersect IBP GRAN_CANAL)

# COMMAND ----------

SECTION = "S2_CANAL"
log("INFO", "=" * 70, SECTION)
log("INFO", "CANAL PROFILING -- All Sources", SECTION)

_CANAL_BASE = f"{DBFS_BASE}/profiling/canal"

# ----------------------------------------------------------
# 2A: SELL_IN V_D_CLIENT
# ----------------------------------------------------------
log("INFO", "2A: Loading SELL_IN V_D_CLIENT (channel + status columns)...", SECTION)
df_client_grouped = run_sf(DB_PRD_MEX, """
    SELECT
        cus_grn_chl_dsc AS gran_canal,
        lv6_hie_cus_dsc AS tipo_cliente,
        cus_chl_are_dsc AS canal_area,
        ptr_1st_cus_dsc AS cliente,
        cus_sal_plt_dsc AS cedis,
        stat_cod        AS estatus,
        COUNT(*)        AS row_count
    FROM PRD_MEX.MEX_DSP_OTC.V_D_CLIENT
    GROUP BY 1, 2, 3, 4, 5, 6
""")
df_client_grouped.cache()
si_client_grouped_rows = df_client_grouped.count()
log("INFO", f"V_D_CLIENT grouped rows: {si_client_grouped_rows:,}", SECTION)

# stat_cod distribution
log("INFO", "stat_cod distribution:", SECTION)
df_stat_dist = (
    df_client_grouped
    .groupBy("estatus")
    .agg(F.sum("row_count").alias("client_count"))
    .orderBy(F.desc("client_count"))
)
_stat_total = df_stat_dist.agg(F.sum("client_count").alias("n")).collect()[0]["n"] or 1
df_stat_dist = df_stat_dist.withColumn(
    "pct",
    F.round(F.col("client_count") / _stat_total * 100, 2)
)
df_stat_dist.show(20, truncate=False)
save_df(df_stat_dist, f"{_CANAL_BASE}/sell_in_stat_cod_distribution.csv", SECTION)

# Active row total
si_active_row_sum = (
    df_client_grouped
    .filter(F.col("estatus") == "A")
    .agg(F.sum("row_count").alias("n"))
    .collect()[0]["n"] or 0
)
log("WARN", "stat_cod filter removed per EDA finding 2026-06-29. Field is dirty (INACTIVO=55.4%, 1=20.4%, ACTIVO=15.2%, 0=8.2%). Full V_D_CLIENT used.", SECTION)
log("INFO", "stat_cod distribution logged above. No active filter applied.", SECTION)

# D1 scoring: load flat active rows
# stat_cod filter REMOVED: dirty field (INACTIVO/ACTIVO/0/1/2/3 mixed). Full dataset used.
log("INFO", "Loading flat active V_D_CLIENT rows for D1 scoring...", SECTION)
df_client_flat = run_sf(DB_PRD_MEX, """
    SELECT cus_grn_chl_dsc, lv6_hie_cus_dsc, cus_chl_are_dsc
    FROM PRD_MEX.MEX_DSP_OTC.V_D_CLIENT
""")
df_client_flat.cache()
si_flat_total = df_client_flat.count()
log("INFO", f"Active V_D_CLIENT flat rows (for D1): {si_flat_total:,}", SECTION)

_si_col_map = {
    "cus_grn_chl_dsc": "gran_canal",
    "lv6_hie_cus_dsc": "tipo_cliente",
    "cus_chl_are_dsc": "canal_area",
}
_si_d1_rows = []

for _col, _alias in _si_col_map.items():
    _null_cnt = df_client_flat.filter(F.col(_col).isNull()).count()
    _dist_cnt = df_client_flat.select(_col).distinct().count()
    _null_pct = round(_null_cnt / max(si_flat_total, 1) * 100, 2)
    _score    = d1_score(_null_pct, _dist_cnt)
    _si_d1_rows.append({
        "source": "SELL_IN", "column": _col, "alias": _alias,
        "total_active": si_flat_total, "null_count": _null_cnt,
        "null_pct": _null_pct, "distinct_count": _dist_cnt, "d1_score": _score,
    })
    log("INFO", f"  {_col:<25} | distinct={_dist_cnt:>5,} | null%={_null_pct:>6.2f}% | D1={_score:.4f}", SECTION)

df_si_d1_profile = spark.createDataFrame(pd.DataFrame(_si_d1_rows))

# Top 30 gran_canal values
log("INFO", "Top 30 cus_grn_chl_dsc (active clients):", SECTION)
df_gran_canal_top30 = (
    df_client_flat
    .groupBy("cus_grn_chl_dsc")
    .count()
    .orderBy(F.desc("count"))
    .limit(30)
)
df_gran_canal_top30.show(30, truncate=False)

save_df(df_si_d1_profile, f"{_CANAL_BASE}/sell_in_canal_d1_profile.csv", SECTION)
save_df(df_gran_canal_top30, f"{_CANAL_BASE}/sell_in_gran_canal_top30.csv", SECTION)

# D1 winner for SELL_IN
_si_winner = max(_si_d1_rows, key=lambda r: r["d1_score"])
log("INFO", f"SELL_IN D1 winner: {_si_winner['column']} (score={_si_winner['d1_score']:.4f})", SECTION)

# ----------------------------------------------------------
# 2B: SELL_OUT VW_D_STORE_RM -- 3-level hierarchy + geography
# ----------------------------------------------------------
log("INFO", "2B: Loading SELL_OUT VW_D_STORE_RM...", SECTION)
df_store_hier = run_sf(DB_PRD_MDP, """
    SELECT
        FORMAT    AS formato,
        CHAIN     AS cadena,
        SUBCHAIN  AS subcadena,
        CITY      AS ciudad,
        STATE     AS estado,
        REGION    AS region,
        CLUSTER   AS cluster,
        COUNT(DISTINCT INT_ID) AS store_count
    FROM PRD_MDP.MDP_DSP.VW_D_STORE_RM
    GROUP BY 1, 2, 3, 4, 5, 6, 7
""")
df_store_hier.cache()
so_hier_rows = df_store_hier.count()
so_total_stores = df_store_hier.agg(F.sum("store_count").alias("n")).collect()[0]["n"] or 0
log("INFO", f"VW_D_STORE_RM grouped rows: {so_hier_rows:,} | total distinct stores: {so_total_stores:,}", SECTION)

# D1 scores for FORMAT, CHAIN, SUBCHAIN
log("INFO", "Loading flat VW_D_STORE_RM for D1 scoring...", SECTION)
df_so_flat = run_sf(DB_PRD_MDP, """
    SELECT FORMAT, CHAIN, SUBCHAIN FROM PRD_MDP.MDP_DSP.VW_D_STORE_RM
""")
df_so_flat.cache()
so_flat_total = df_so_flat.count()

_so_d1_rows = []
for _col in ["FORMAT", "CHAIN", "SUBCHAIN"]:
    _null_cnt = df_so_flat.filter(F.col(_col).isNull()).count()
    _dist_cnt = df_so_flat.select(_col).distinct().count()
    _null_pct = round(_null_cnt / max(so_flat_total, 1) * 100, 2)
    _score    = d1_score(_null_pct, _dist_cnt)
    _so_d1_rows.append({
        "source": "SELL_OUT", "column": _col,
        "total_rows": so_flat_total, "null_count": _null_cnt,
        "null_pct": _null_pct, "distinct_count": _dist_cnt, "d1_score": _score,
    })
    log("INFO", f"  {_col:<12} | distinct={_dist_cnt:>5,} | null%={_null_pct:>6.2f}% | D1={_score:.4f}", SECTION)

_subchain_null_pct = next((r["null_pct"] for r in _so_d1_rows if r["column"] == "SUBCHAIN"), 0.0)
warn(
    _subchain_null_pct > 30.0,
    f"SELL_OUT SUBCHAIN null rate = {_subchain_null_pct:.2f}% > 30% -- unreliable for D1 profiling.",
    SECTION,
)

# FORMAT distinct values sorted by store_count desc
log("INFO", "FORMAT distinct values by store_count:", SECTION)
df_format_dist = (
    df_store_hier
    .groupBy("formato")
    .agg(F.sum("store_count").alias("store_count"))
    .orderBy(F.desc("store_count"))
)
df_format_dist.show(30, truncate=False)

# FORMAT -> CHAIN top 20 combinations
log("INFO", "FORMAT -> CHAIN hierarchy (top 20 by store_count):", SECTION)
df_format_chain_top20 = (
    df_store_hier
    .groupBy("formato", "cadena")
    .agg(F.sum("store_count").alias("store_count"))
    .orderBy(F.desc("store_count"))
    .limit(20)
)
df_format_chain_top20.show(20, truncate=False)

# Store geo distinct counts
log("INFO", "Store geography distinct counts:", SECTION)
_geo_summary = []
for _col, _alias in [("ciudad", "CITY"), ("estado", "STATE"), ("region", "REGION"), ("cluster", "CLUSTER")]:
    _dist = df_store_hier.select(_col).distinct().count()
    _geo_summary.append({"geo_column": _alias, "distinct_count": _dist})
    log("INFO", f"  {_alias:<10} | distinct={_dist:>5,}", SECTION)
df_geo_summary = spark.createDataFrame(pd.DataFrame(_geo_summary))

save_df(df_store_hier, f"{_CANAL_BASE}/sell_out_store_hierarchy_profile.csv", SECTION)
save_df(spark.createDataFrame(pd.DataFrame(_so_d1_rows)), f"{_CANAL_BASE}/sell_out_store_d1_profile.csv", SECTION)
save_df(df_geo_summary, f"{_CANAL_BASE}/sell_out_store_geo_summary.csv", SECTION)

# D1 winner for SELL_OUT
_so_winner = max(_so_d1_rows, key=lambda r: r["d1_score"])
log("INFO", f"SELL_OUT D1 winner: {_so_winner['column']} (score={_so_winner['d1_score']:.4f})", SECTION)

# ----------------------------------------------------------
# 2C: IBP VW_FACT_DANONE_IBP -- 5-level hierarchy
# ----------------------------------------------------------
log("INFO", "2C: Loading IBP VW_FACT_DANONE_IBP (NOMBRE_ETIQUETA='REAL')...", SECTION)
df_ibp_hier = run_sf(DB_PRD_MDP, """
    SELECT
        GRAN_CANAL, CANAL, GRUPO, CADENA, FM,
        COUNT(*) AS row_count
    FROM PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP
    WHERE NOMBRE_ETIQUETA = 'REAL'
    GROUP BY 1, 2, 3, 4, 5
""")
df_ibp_hier.cache()
ibp_hier_rows = df_ibp_hier.count()
ibp_total_rows = df_ibp_hier.agg(F.sum("row_count").alias("n")).collect()[0]["n"] or 0
log("INFO", f"IBP hierarchy grouped rows: {ibp_hier_rows:,} | total fact rows: {ibp_total_rows:,}", SECTION)

log("INFO", "Loading flat IBP rows for D1 scoring...", SECTION)
df_ibp_flat = run_sf(DB_PRD_MDP, """
    SELECT GRAN_CANAL, CANAL, GRUPO, CADENA, FM
    FROM PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP
    WHERE NOMBRE_ETIQUETA = 'REAL'
""")
df_ibp_flat.cache()
ibp_flat_total = df_ibp_flat.count()

_ibp_d1_rows = []
for _col in ["GRAN_CANAL", "CANAL", "GRUPO", "CADENA", "FM"]:
    _null_cnt = df_ibp_flat.filter(F.col(_col).isNull()).count()
    _dist_cnt = df_ibp_flat.select(_col).distinct().count()
    _null_pct = round(_null_cnt / max(ibp_flat_total, 1) * 100, 2)
    _score    = d1_score(_null_pct, _dist_cnt)
    _ibp_d1_rows.append({
        "source": "IBP", "column": _col,
        "total_rows": ibp_flat_total, "null_count": _null_cnt,
        "null_pct": _null_pct, "distinct_count": _dist_cnt, "d1_score": _score,
    })
    log("INFO", f"  {_col:<12} | distinct={_dist_cnt:>5,} | null%={_null_pct:>6.2f}% | D1={_score:.4f}", SECTION)

# GRAN_CANAL -> CANAL combinations (all distinct)
log("INFO", "GRAN_CANAL -> CANAL distinct hierarchy:", SECTION)
df_ibp_gc_canal = (
    df_ibp_flat
    .select("GRAN_CANAL", "CANAL")
    .distinct()
    .orderBy("GRAN_CANAL", "CANAL")
)
df_ibp_gc_canal.show(50, truncate=False)

# CADENA distinct sorted alphabetically
log("INFO", "IBP CADENA distinct values (alphabetical):", SECTION)
df_ibp_cadena_dist = (
    df_ibp_flat
    .select("CADENA")
    .distinct()
    .orderBy("CADENA")
)
df_ibp_cadena_dist.show(50, truncate=False)

save_df(df_ibp_hier, f"{_CANAL_BASE}/ibp_canal_hierarchy_profile.csv", SECTION)
save_df(spark.createDataFrame(pd.DataFrame(_ibp_d1_rows)), f"{_CANAL_BASE}/ibp_canal_d1_profile.csv", SECTION)
save_df(df_ibp_gc_canal, f"{_CANAL_BASE}/ibp_gran_canal_canal_combinations.csv", SECTION)

_ibp_gran_canal_distinct = df_ibp_flat.select("GRAN_CANAL").distinct().count()
log("INFO", f"IBP GRAN_CANAL distinct values: {_ibp_gran_canal_distinct}", SECTION)

# ----------------------------------------------------------
# 2D: WASTE VW_WASTE -- CANAL column (FUENTE='TOPLINE')
# ----------------------------------------------------------
log("INFO", "2D: Loading WASTE VW_WASTE CANAL (FUENTE=TOPLINE)...", SECTION)
df_waste_canal = run_sf(DB_PRD_MDP, """
    SELECT
        CANAL AS canal_waste,
        COUNT(*) AS row_count
    FROM PRD_MDP.MDP_STG.VW_WASTE
    WHERE UPPER(TRIM(FUENTE)) = 'TOPLINE'
      AND CANAL IS NOT NULL
    GROUP BY 1
    ORDER BY 2 DESC
""")
df_waste_canal.cache()
waste_distinct_canal = df_waste_canal.count()
log("INFO", f"WASTE CANAL distinct values (TOPLINE): {waste_distinct_canal}", SECTION)
df_waste_canal.show(30, truncate=False)

df_waste_flat = run_sf(DB_PRD_MDP, """
    SELECT CANAL FROM PRD_MDP.MDP_STG.VW_WASTE WHERE UPPER(TRIM(FUENTE)) = 'TOPLINE'
""")
waste_flat_total  = df_waste_flat.count()
waste_null_cnt    = df_waste_flat.filter(F.col("CANAL").isNull()).count()
waste_null_pct    = round(waste_null_cnt / max(waste_flat_total, 1) * 100, 2)
waste_d1          = d1_score(waste_null_pct, waste_distinct_canal)
log("INFO", f"WASTE CANAL D1={waste_d1:.4f} | null%={waste_null_pct:.2f}% | distinct={waste_distinct_canal}", SECTION)

save_df(df_waste_canal, f"{_CANAL_BASE}/waste_canal_profile.csv", SECTION)

# ----------------------------------------------------------
# 2E: Unified canal seed -- overlap SELL_IN intersect IBP GRAN_CANAL
# ----------------------------------------------------------
log("INFO", "2E: Building unified canal seed...", SECTION)

# Collect distinct value sets for set operations
_ibp_gc_set = set(
    r["GRAN_CANAL"].strip().upper()
    for r in df_ibp_flat.select("GRAN_CANAL").distinct().collect()
    if r["GRAN_CANAL"] is not None
)
_si_gc_set = set(
    r["cus_grn_chl_dsc"].strip().upper()
    for r in df_client_flat.select("cus_grn_chl_dsc").distinct().collect()
    if r["cus_grn_chl_dsc"] is not None
)
_so_format_set = set(
    r["FORMAT"].strip().upper()
    for r in df_so_flat.select("FORMAT").distinct().collect()
    if r["FORMAT"] is not None
)
_waste_canal_set = set(
    r["canal_waste"].strip().upper()
    for r in df_waste_canal.select("canal_waste").collect()
    if r["canal_waste"] is not None
)

_gc_intersection = _ibp_gc_set & _si_gc_set
_gc_union        = _ibp_gc_set | _si_gc_set
_overlap_pct     = round(len(_gc_intersection) / max(len(_gc_union), 1) * 100, 2)

log("INFO", f"SELL_IN cus_grn_chl_dsc distinct: {len(_si_gc_set)}", SECTION)
log("INFO", f"IBP GRAN_CANAL distinct: {len(_ibp_gc_set)}", SECTION)
log("INFO", f"INTERSECTION (both): {len(_gc_intersection)} -- {sorted(_gc_intersection)}", SECTION)
log("INFO", f"IBP_ONLY: {sorted(_ibp_gc_set - _si_gc_set)}", SECTION)
log("INFO", f"SELL_IN_ONLY: {sorted(_si_gc_set - _ibp_gc_set)}", SECTION)
log("INFO", f"Jaccard overlap: {_overlap_pct:.2f}%", SECTION)

# Build unified seed: union all promoted canal values
_unified_rows = []
for v in sorted(_si_gc_set):
    _unified_rows.append({
        "canal_value": v,
        "source": "SELL_IN_cus_grn_chl_dsc",
        "in_ibp_gran_canal": v in _ibp_gc_set,
    })
for v in sorted(_ibp_gc_set - _si_gc_set):
    _unified_rows.append({
        "canal_value": v,
        "source": "IBP_GRAN_CANAL_only",
        "in_ibp_gran_canal": True,
    })
for v in sorted(_so_format_set):
    _unified_rows.append({
        "canal_value": v,
        "source": "SELL_OUT_FORMAT",
        "in_ibp_gran_canal": False,
    })

df_canal_unified = spark.createDataFrame(pd.DataFrame(_unified_rows))
save_df(df_canal_unified, f"{_CANAL_BASE}/canal_unified_seed.csv", SECTION)
log("INFO", f"Canal unified seed written: {len(_unified_rows)} rows", SECTION)

log("INFO", "Section 2 complete.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3 -- MARKET Profiling
# MAGIC
# MAGIC Profile all 4 Nielsen MKT_DIM tables:
# MAGIC - EDP       (VW_IR_YOG_GEL_MT_NLSN_MKT_DIM)
# MAGIC - PB        (VW_SUST_LECHE_ST_NLSN_MKT_DIM)
# MAGIC - WATER_ST  (VW_IND_AGUA_BNF_ST_NLSN_MKT_DIM)
# MAGIC - WATER_RT  (VW_IND_AGUA_BNF_RT_NLSN_MKT_DIM)
# MAGIC
# MAGIC Overlay M1 sign-off file logs/signoff_03_nielsen_markets.csv when present.

# COMMAND ----------

SECTION = "S3_MARKET"
log("INFO", "=" * 70, SECTION)
log("INFO", "MARKET PROFILING -- 4 Nielsen MKT_DIM tables", SECTION)

_MARKET_BASE = f"{DBFS_BASE}/profiling/market"

_NIELSEN_MKT_TABLES = {
    "EDP":      "PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_MKT_DIM",
    "PB":       "PRD_MEX.MEX_DSP_DPH_MKT.VW_SUST_LECHE_ST_NLSN_MKT_DIM",
    "WATER_ST": "PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_ST_NLSN_MKT_DIM",
    "WATER_RT": "PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_RT_NLSN_MKT_DIM",
}

_mkt_dfs   = {}
_mkt_d1_rows = []
_mkt_sets  = {}

for _dataset, _table in _NIELSEN_MKT_TABLES.items():
    log("INFO", f"Loading {_dataset} from {_table}...", SECTION)
    # Attempt with MRKT_DSC_LONG; fall back to NULL alias if column absent
    try:
        _df = run_sf(DB_PRD_MEX, f"""
            SELECT
                MRKT_DSC_SHRT,
                MRKT_DSC_LONG,
                COUNT(DISTINCT "market_id") AS market_id_count,
                '{_dataset}'              AS nielsen_dataset
            FROM {_table}
            WHERE MRKT_DSC_SHRT IS NOT NULL
            GROUP BY 1, 2
        """)
        # Trigger schema validation
        _ = _df.schema
    except Exception:
        log("WARNING", f"{_dataset}: MRKT_DSC_LONG not found or error; retrying without it.", SECTION)
        _df = run_sf(DB_PRD_MEX, f"""
            SELECT
                MRKT_DSC_SHRT,
                CAST(NULL AS VARCHAR) AS MRKT_DSC_LONG,
                COUNT(DISTINCT "market_id") AS market_id_count,
                '{_dataset}'              AS nielsen_dataset
            FROM {_table}
            WHERE MRKT_DSC_SHRT IS NOT NULL
            GROUP BY 1
        """)
    _df.cache()
    _cnt = _df.count()
    log("INFO", f"  {_dataset}: {_cnt:,} distinct MRKT_DSC_SHRT groups", SECTION)

    # D1 score for MRKT_DSC_SHRT
    _flat = run_sf(DB_PRD_MEX, f"SELECT MRKT_DSC_SHRT FROM {_table}")
    _flat_total = _flat.count()
    _null_cnt   = _flat.filter(F.col("MRKT_DSC_SHRT").isNull()).count()
    _dist_cnt   = _flat.filter(F.col("MRKT_DSC_SHRT").isNotNull()).select("MRKT_DSC_SHRT").distinct().count()
    _null_pct   = round(_null_cnt / max(_flat_total, 1) * 100, 2)
    _score      = d1_score(_null_pct, _dist_cnt)
    _mkt_d1_rows.append({
        "nielsen_dataset": _dataset, "column": "MRKT_DSC_SHRT",
        "total_rows": _flat_total, "null_count": _null_cnt,
        "null_pct": _null_pct, "distinct_count": _dist_cnt, "d1_score": _score,
    })
    log("INFO", f"  {_dataset} D1={_score:.4f} | distinct={_dist_cnt} | null%={_null_pct:.2f}%", SECTION)

    # Collect distinct MRKT_DSC_SHRT values for set operations
    _mkt_sets[_dataset] = set(
        r["MRKT_DSC_SHRT"].strip().upper()
        for r in _df.select("MRKT_DSC_SHRT").distinct().collect()
        if r["MRKT_DSC_SHRT"] is not None
    )
    _mkt_dfs[_dataset] = _df

# ----------------------------------------------------------
# 3A: Cross-dataset overlap analysis
# ----------------------------------------------------------
log("INFO", "Computing cross-dataset MRKT_DSC_SHRT overlap...", SECTION)

_all_keys = (
    _mkt_sets["EDP"]
    | _mkt_sets["PB"]
    | _mkt_sets["WATER_ST"]
    | _mkt_sets["WATER_RT"]
)
_in_all_4 = (
    _mkt_sets["EDP"]
    & _mkt_sets["PB"]
    & _mkt_sets["WATER_ST"]
    & _mkt_sets["WATER_RT"]
)
_in_some = _all_keys - _in_all_4

_dataset_unique = {}
for _d, _s in _mkt_sets.items():
    _others_union = set()
    for _d2, _s2 in _mkt_sets.items():
        if _d2 != _d:
            _others_union |= _s2
    _dataset_unique[_d] = _s - _others_union

log("INFO", f"Total distinct MRKT_DSC_SHRT across all 4 datasets: {len(_all_keys)}", SECTION)
log("INFO", f"IN_ALL_4 (common keys): {len(_in_all_4)}", SECTION)
log("INFO", f"IN_SOME  (partial):     {len(_in_some)}", SECTION)
for _d, _u in _dataset_unique.items():
    log("INFO", f"  {_d} dataset-unique keys: {len(_u)}", SECTION)

# ----------------------------------------------------------
# 3B: Union all 4 into full market profile
# ----------------------------------------------------------
df_mkt_union = (
    _mkt_dfs["EDP"]
    .union(_mkt_dfs["PB"])
    .union(_mkt_dfs["WATER_ST"])
    .union(_mkt_dfs["WATER_RT"])
)
df_mkt_union.cache()

# ----------------------------------------------------------
# 3C: Load M1 sign-off file (if present)
# ----------------------------------------------------------
log("INFO", "Loading M1 sign-off file...", SECTION)
_signoff_path = os.path.join(_REPO_ROOT, "logs", "signoff_03_nielsen_markets.csv")
_confirmed_markets: set = set()

if os.path.exists(_signoff_path):
    try:
        _so_pd = pd.read_csv(_signoff_path)
        _col_check = [c for c in _so_pd.columns if "MRKT" in c.upper() or "market" in c.lower()]
        _key_col   = _col_check[0] if _col_check else _so_pd.columns[0]
        _confirmed_markets = {str(v).strip().upper() for v in _so_pd[_key_col].dropna()}
        log("INFO", f"M1 sign-off loaded from '{_key_col}': {len(_confirmed_markets)} confirmed market keys.", SECTION)
    except Exception as _mso_err:
        warn(True, f"signoff_03_nielsen_markets.csv parse error: {_mso_err}", SECTION)
else:
    warn(True, "logs/signoff_03_nielsen_markets.csv NOT FOUND. All markets classified as PENDING.", SECTION)

# Classify CONFIRMED / PENDING
_confirmed_mkt_bc = spark.sparkContext.broadcast(_confirmed_markets)


def _classify_market(mrkt_key):
    if mrkt_key is None:
        return "PENDING"
    return "CONFIRMED" if mrkt_key.strip().upper() in _confirmed_mkt_bc.value else "PENDING"


_classify_market_udf = F.udf(_classify_market)

df_mkt_classified = df_mkt_union.withColumn(
    "m1_status", _classify_market_udf(F.col("MRKT_DSC_SHRT"))
)
df_mkt_classified.cache()

_mkt_confirmed  = df_mkt_classified.filter(F.col("m1_status") == "CONFIRMED").select("MRKT_DSC_SHRT").distinct().count()
_mkt_pending    = df_mkt_classified.filter(F.col("m1_status") == "PENDING").select("MRKT_DSC_SHRT").distinct().count()
_mkt_total      = _mkt_confirmed + _mkt_pending
_mkt_cov_pct    = round(_mkt_confirmed / max(_mkt_total, 1) * 100, 2)
log("INFO", f"Market M1 coverage: confirmed={_mkt_confirmed} | pending={_mkt_pending} | coverage={_mkt_cov_pct:.2f}%", SECTION)

# Pending-only DataFrame
df_mkt_pending = (
    df_mkt_classified
    .filter(F.col("m1_status") == "PENDING")
    .select("MRKT_DSC_SHRT", "MRKT_DSC_LONG", "nielsen_dataset", "market_id_count")
    .orderBy("nielsen_dataset", "MRKT_DSC_SHRT")
)

# Write outputs
save_df(df_mkt_classified, f"{_MARKET_BASE}/market_all_datasets_profile.csv", SECTION)
save_df(df_mkt_pending, f"{_MARKET_BASE}/market_pending_classification.csv", SECTION)
save_df(spark.createDataFrame(pd.DataFrame(_mkt_d1_rows)), f"{_MARKET_BASE}/market_d1_profile.csv", SECTION)

print("\n" + "=" * 60)
print("MARKET -- ALL DATASETS PROFILE")
print("=" * 60)
display(df_mkt_classified)

print("\n" + "=" * 60)
print("MARKET -- PENDING CLASSIFICATION")
print("=" * 60)
display(df_mkt_pending)

log("INFO", "Section 3 complete.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4 -- UPC Profiling
# MAGIC
# MAGIC 4A: SAP V_D_ITEM base catalog -- CBU breakdown, EAN null rate
# MAGIC 4B: SELL_OUT VW_D_PRODUCT_RM -- brand_owner / import_id coverage
# MAGIC 4C: Nielsen PROD_DIM UPC coverage (EDP, PB, WATER_ST -- Water Retail EXCLUDED)
# MAGIC 4D: P0 Bridge simulation (Nielsen PRDC_CD -> SAP SKU_EAN_COD)
# MAGIC 4E: P1 Bridge simulation (SELL_OUT INT_ID -> SAP SKU_EAN_COD)
# MAGIC
# MAGIC Gate: blocker() if Danone NULL EAN exists in V_D_ITEM

# COMMAND ----------

SECTION = "S4_UPC"
log("INFO", "=" * 70, SECTION)
log("INFO", "UPC PROFILING -- SAP, SELL_OUT, Nielsen P0/P1 bridges", SECTION)

_UPC_BASE = f"{DBFS_BASE}/profiling/upc"

# ----------------------------------------------------------
# 4A: SAP V_D_ITEM -- CBU breakdown + EAN coverage
# ----------------------------------------------------------
log("INFO", "4A: Loading SAP V_D_ITEM EAN coverage by CBU...", SECTION)
df_sap_cbu = run_sf(DB_PRD_MEX, """
    SELECT
        CBU,
        COUNT(DISTINCT MAT_IDT)     AS mat_idt_count,
        COUNT(DISTINCT SKU_EAN_COD) AS ean_count,
        SUM(CASE WHEN SKU_EAN_COD IS NULL THEN 1 ELSE 0 END)    AS null_ean_count,
        ROUND(
            SUM(CASE WHEN SKU_EAN_COD IS NULL THEN 1.0 ELSE 0 END)
            / COUNT(*) * 100, 2
        ) AS null_ean_pct
    FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM
    GROUP BY 1
""")
df_sap_cbu.cache()
log("INFO", "SAP V_D_ITEM by CBU:", SECTION)
df_sap_cbu.show(30, truncate=False)

_sap_agg = run_sf(DB_PRD_MEX, """
    SELECT
        COUNT(DISTINCT MAT_IDT)      AS total_mat_idt,
        COUNT(DISTINCT SKU_EAN_COD)  AS total_ean,
        SUM(CASE WHEN SKU_EAN_COD IS NULL THEN 1 ELSE 0 END)    AS total_null_ean,
        COUNT(*)                                                  AS total_rows,
        ROUND(
            SUM(CASE WHEN SKU_EAN_COD IS NULL THEN 1.0 ELSE 0 END)
            / COUNT(*) * 100, 2
        ) AS null_ean_pct
    FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM
""").collect()[0]

_sap_mat_total    = int(_sap_agg["TOTAL_MAT_IDT"])
_sap_null_ean     = int(_sap_agg["TOTAL_NULL_EAN"])
_sap_null_ean_pct = float(_sap_agg["NULL_EAN_PCT"])
_sap_ean_cov_pct  = round(100.0 - _sap_null_ean_pct, 2)

log("INFO", f"SAP totals: MAT_IDT={_sap_mat_total:,} | total_EAN={_sap_agg['TOTAL_EAN']:,} | "
            f"null_EAN={_sap_null_ean:,} | null_EAN%={_sap_null_ean_pct:.2f}% | EAN_coverage={_sap_ean_cov_pct:.2f}%", SECTION)

blocker(
    _sap_null_ean > 0,
    f"SAP V_D_ITEM has {_sap_null_ean:,} rows with NULL SKU_EAN_COD. "
    "Danone products without EAN cannot participate in the P0 UPC bridge. Investigate.",
    SECTION,
)

save_df(df_sap_cbu, f"{_UPC_BASE}/sap_item_ean_coverage_by_cbu.csv", SECTION)

# ----------------------------------------------------------
# 4B: SELL_OUT VW_D_PRODUCT_RM
# ----------------------------------------------------------
log("INFO", "4B: Loading SELL_OUT VW_D_PRODUCT_RM product coverage by CBU_ID...", SECTION)
df_so_product = run_sf(DB_PRD_MDP, """
    SELECT
        CBU_ID,
        COUNT(DISTINCT INT_ID)    AS int_id_count,
        COUNT(DISTINCT IMPORT_ID) AS import_id_count,
        SUM(CASE WHEN IMPORT_ID IS NULL THEN 1 ELSE 0 END) AS null_import_id,
        COUNT(DISTINCT BRAND)     AS brand_distinct
    FROM PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM
    GROUP BY 1
""")
df_so_product.cache()
log("INFO", "SELL_OUT product coverage by CBU_ID:", SECTION)
df_so_product.show(30, truncate=False)
save_df(df_so_product, f"{_UPC_BASE}/sellout_product_coverage_by_cbu.csv", SECTION)

_so_agg = run_sf(DB_PRD_MDP, """
    SELECT
        COUNT(DISTINCT INT_ID)    AS total_int_id,
        COUNT(DISTINCT IMPORT_ID) AS total_import_id,
        SUM(CASE WHEN IMPORT_ID IS NULL THEN 1 ELSE 0 END) AS null_import_id
    FROM PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM
""").collect()[0]
log("INFO", f"SELL_OUT totals: INT_ID={_so_agg['TOTAL_INT_ID']:,} | IMPORT_ID={_so_agg['TOTAL_IMPORT_ID']:,} | null_IMPORT_ID={_so_agg['NULL_IMPORT_ID']:,}", SECTION)

# ----------------------------------------------------------
# 4C: Nielsen PROD_DIM UPC coverage (3 datasets -- Water Retail EXCLUDED)
# ----------------------------------------------------------
log("INFO", "4C: Nielsen PROD_DIM UPC coverage (EDP, PB_SCANTRACK, WATER_SCANTRACK)...", SECTION)

_NIELSEN_PROD_CONFIG = {
    "EDP": {
        "table":     "PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_PROD_DIM",
        "brand_col": "INP_56985",
        "where":     '"hierarchy_level" = 11',
    },
    "PB_SCANTRACK": {
        "table":     "PRD_MEX.MEX_DSP_DPH_MKT.VW_SUST_LECHE_ST_NLSN_PROD_DIM",
        "brand_col": "INP_56985",
        "where":     None,
    },
    "WATER_SCANTRACK": {
        "table":     "PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_ST_NLSN_PROD_DIM",
        "brand_col": "CSTM_310589",
        "where":     '"hierarchy_level" = 11',
    },
}

_nielsen_upc_rows = []
for _ds, _cfg in _NIELSEN_PROD_CONFIG.items():
    _where_clause = f"WHERE {_cfg['where']}" if _cfg["where"] else ""
    _df = run_sf(DB_PRD_MEX, f"""
        SELECT
            COUNT(DISTINCT PRDC_CD)            AS distinct_prdc_cd,
            SUM(CASE WHEN PRDC_CD IS NULL THEN 1 ELSE 0 END) AS null_prdc_cd,
            COUNT(DISTINCT {_cfg['brand_col']}) AS distinct_brands,
            '{_ds}'                             AS nielsen_dataset
        FROM {_cfg['table']}
        {_where_clause}
    """)
    _row = _df.collect()[0]
    _nielsen_upc_rows.append({
        "nielsen_dataset":  _ds,
        "distinct_prdc_cd": int(_row["DISTINCT_PRDC_CD"]),
        "null_prdc_cd":     int(_row["NULL_PRDC_CD"]),
        "distinct_brands":  int(_row["DISTINCT_BRANDS"]),
    })
    log("INFO", f"  {_ds:<20} | distinct_prdc_cd={_row['DISTINCT_PRDC_CD']:,} | null_prdc_cd={_row['NULL_PRDC_CD']:,} | brands={_row['DISTINCT_BRANDS']:,}", SECTION)

df_nielsen_upc = spark.createDataFrame(pd.DataFrame(_nielsen_upc_rows))
save_df(df_nielsen_upc, f"{_UPC_BASE}/nielsen_prod_upc_coverage.csv", SECTION)

# ----------------------------------------------------------
# 4D: P0 Bridge simulation (Nielsen PRDC_CD -> SAP SKU_EAN_COD)
# ----------------------------------------------------------
log("INFO", "4D: P0 Bridge simulation -- Nielsen PRDC_CD <-> SAP SKU_EAN_COD...", SECTION)

_p0_results = []
for _ds, _cfg in _NIELSEN_PROD_CONFIG.items():
    _where_extra = f"AND {_cfg['where']}" if _cfg["where"] else ""
    log("INFO", f"  Running P0 bridge for {_ds}...", SECTION)
    df_p0 = run_sf(DB_PRD_MEX, f"""
        SELECT
            n.PRDC_CD,
            s.MAT_IDT,
            s.SKU_EAN_COD,
            '{_ds}' AS nielsen_dataset,
            CASE
                WHEN s.SKU_EAN_COD IS NOT NULL THEN 'P0_MATCH'
                ELSE 'P0_UNMATCHED'
            END AS match_status
        FROM {_cfg['table']} n
        LEFT JOIN PRD_MEX.MEX_DSP_OTC.V_D_ITEM s
            ON TRIM(n.PRDC_CD) = TRIM(TO_VARCHAR(s.SKU_EAN_COD))
        WHERE n.PRDC_CD IS NOT NULL
          {_where_extra}
    """)
    df_p0.cache()
    _p0_total   = df_p0.count()
    _p0_matched = df_p0.filter(F.col("match_status") == "P0_MATCH").count()
    _p0_rate    = round(_p0_matched / max(_p0_total, 1) * 100, 2)
    log("INFO", f"  {_ds}: P0 match rate = {_p0_rate:.2f}% ({_p0_matched:,}/{_p0_total:,})", SECTION)
    _p0_results.append({
        "nielsen_dataset":  _ds,
        "total_prdc_cd":    _p0_total,
        "p0_matched":       _p0_matched,
        "p0_match_rate_pct": _p0_rate,
    })
    save_df(df_p0, f"{_UPC_BASE}/p0_bridge_{_ds.lower()}.csv", SECTION)

df_p0_summary = spark.createDataFrame(pd.DataFrame(_p0_results))
save_df(df_p0_summary, f"{_UPC_BASE}/p0_bridge_summary.csv", SECTION)
log("INFO", "P0 Bridge Summary:", SECTION)
df_p0_summary.show(10, truncate=False)

_p0_total_all   = sum(r["total_prdc_cd"] for r in _p0_results)
_p0_matched_all = sum(r["p0_matched"] for r in _p0_results)
_p0_overall_pct = round(_p0_matched_all / max(_p0_total_all, 1) * 100, 2)
log("INFO", f"Overall P0 match rate (3 datasets combined): {_p0_overall_pct:.2f}%", SECTION)

# ----------------------------------------------------------
# 4E: P1 Bridge simulation (SELL_OUT INT_ID -> SAP SKU_EAN_COD)
# ----------------------------------------------------------
log("INFO", "4E: P1 Bridge simulation -- SELL_OUT INT_ID <-> SAP SKU_EAN_COD...", SECTION)
_df_p1_sellout = run_sf(DB_PRD_MDP, """
    SELECT INT_ID, CBU_ID
    FROM PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM
    WHERE INT_ID IS NOT NULL
""")
_df_p1_sap = run_sf(DB_PRD_MEX, """
    SELECT MAT_IDT, SKU_EAN_COD
    FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM
    WHERE SKU_EAN_COD IS NOT NULL
""")
df_p1 = (
    _df_p1_sellout
    .withColumn("_join_key", F.trim(F.col("INT_ID").cast("string")))
    .join(
        _df_p1_sap.withColumn("_join_key", F.trim(F.col("SKU_EAN_COD").cast("string"))),
        on="_join_key",
        how="left"
    )
    .withColumn("match_status", F.when(F.col("SKU_EAN_COD").isNotNull(), F.lit("P1_MATCH")).otherwise(F.lit("P1_UNMATCHED")))
    .select("INT_ID", "MAT_IDT", "SKU_EAN_COD", "CBU_ID", "match_status")
)
df_p1.cache()
_p1_total    = df_p1.count()
_p1_matched  = df_p1.filter(F.col("match_status") == "P1_MATCH").count()
_p1_rate     = round(_p1_matched / max(_p1_total, 1) * 100, 2)
log("INFO", f"P1 match rate: {_p1_rate:.2f}% ({_p1_matched:,}/{_p1_total:,})", SECTION)

# CBU-level breakdown
df_p1_cbu = (
    df_p1
    .groupBy("CBU_ID", "match_status")
    .count()
    .orderBy("CBU_ID", "match_status")
)
log("INFO", "P1 Bridge by CBU_ID:", SECTION)
df_p1_cbu.show(30, truncate=False)

save_df(df_p1, f"{_UPC_BASE}/p1_bridge_full.csv", SECTION)
save_df(df_p1_cbu, f"{_UPC_BASE}/p1_bridge_by_cbu.csv", SECTION)

log("INFO", "Section 4 complete.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5 -- Summary + MDM Enrichment Output
# MAGIC
# MAGIC Writes:
# MAGIC - catalog_profiling_summary_v6.json  -- machine-readable MDM handoff
# MAGIC - catalog_profiling_summary_v6_flat.csv -- flat key=value CSV for downstream enrichment
# MAGIC
# MAGIC Then flushes the full run log and raises if any hard blockers remain.

# COMMAND ----------

SECTION = "S5_SUMMARY"
log("INFO", "=" * 70, SECTION)
log("INFO", "BUILDING MDM ENRICHMENT SUMMARY", SECTION)

# ----------------------------------------------------------
# 5A: Assemble summary dict
# ----------------------------------------------------------
summary = {
    "catalog_version": CATALOG_VERSION,
    "run_date": RUN_DATE,
    "marca": {
        "total_raw_variants": _marca_total_raw,
        "confirmed":          _marca_confirmed_total,
        "unmapped":           _marca_unmapped_total,
        "coverage_pct":       _marca_coverage_pct,
        "by_source":          _marca_by_source_dict,
    },
    "canal": {
        "sell_in_d1_winner":         _si_winner["column"],
        "sell_in_d1_score":          _si_winner["d1_score"],
        "sell_out_d1_winner":        _so_winner["column"],
        "sell_out_d1_score":         _so_winner["d1_score"],
        "ibp_gran_canal_distinct":   _ibp_gran_canal_distinct,
        "sell_out_format_distinct":  len(_so_format_set),
        "waste_canal_distinct":      waste_distinct_canal,
        "ibp_sellin_gc_overlap_pct": _overlap_pct,
    },
    "market": {
        "total_distinct_mrkt_keys": len(_all_keys),
        "in_all_4_datasets":        len(_in_all_4),
        "in_some_datasets":         len(_in_some),
        "confirmed_m1":             _mkt_confirmed,
        "pending":                  _mkt_pending,
        "m1_coverage_pct":          _mkt_cov_pct,
    },
    "upc": {
        "sap_mat_idt_total":    _sap_mat_total,
        "sap_null_ean_count":   _sap_null_ean,
        "sap_null_ean_pct":     _sap_null_ean_pct,
        "sap_ean_coverage_pct": _sap_ean_cov_pct,
        "p0_match_rate_pct":    _p0_overall_pct,
        "p1_total_records":     _p1_total,
        "p1_matched":           _p1_matched,
        "p1_match_rate_pct":    _p1_rate,
    },
    "run_metadata": {
        "hard_blocker_count": len(_HARD_BLOCKERS),
        "warning_count":      len(_WARNINGS),
        "blockers":           _HARD_BLOCKERS,
        "warnings":           _WARNINGS,
    },
}

# ----------------------------------------------------------
# 5B: Print full summary
# ----------------------------------------------------------
print("\n" + "=" * 70)
print(f"CATALOG PROFILING SUMMARY  v{CATALOG_VERSION}  --  {RUN_DATE}")
print("=" * 70)
print(json.dumps(summary, indent=2))

# ----------------------------------------------------------
# 5C: Write JSON to DBFS and repo
# ----------------------------------------------------------
_summary_json_str = json.dumps(summary, indent=2)
_json_dbfs_path   = f"{DBFS_BASE}/profiling/summary/catalog_profiling_summary_v6.json"
dbutils.fs.put(_json_dbfs_path, _summary_json_str, overwrite=True)
log("INFO", f"JSON summary written to DBFS: {_json_dbfs_path}", SECTION)

_json_repo_path = os.path.join(REPO_LOGS_DIR, "catalog_profiling_summary_v6.json")
with open(_json_repo_path, "w", encoding="utf-8") as _jf:
    json.dump(summary, _jf, indent=2)
log("INFO", f"JSON summary saved to repo: {_json_repo_path}", SECTION)

# ----------------------------------------------------------
# 5D: Write flat CSV for MDM enrichment
# ----------------------------------------------------------
_flat_rows = [
    {"key": "catalog_version",              "value": CATALOG_VERSION},
    {"key": "run_date",                     "value": RUN_DATE},
    {"key": "marca.total_raw_variants",     "value": str(_marca_total_raw)},
    {"key": "marca.confirmed",              "value": str(_marca_confirmed_total)},
    {"key": "marca.unmapped",               "value": str(_marca_unmapped_total)},
    {"key": "marca.coverage_pct",           "value": str(_marca_coverage_pct)},
    {"key": "canal.sell_in_d1_winner",      "value": _si_winner["column"]},
    {"key": "canal.sell_in_d1_score",       "value": str(_si_winner["d1_score"])},
    {"key": "canal.sell_out_d1_winner",     "value": _so_winner["column"]},
    {"key": "canal.sell_out_d1_score",      "value": str(_so_winner["d1_score"])},
    {"key": "canal.ibp_gran_canal_distinct","value": str(_ibp_gran_canal_distinct)},
    {"key": "canal.sell_out_format_distinct","value": str(len(_so_format_set))},
    {"key": "canal.waste_canal_distinct",   "value": str(waste_distinct_canal)},
    {"key": "canal.ibp_sellin_gc_overlap_pct", "value": str(_overlap_pct)},
    {"key": "market.total_distinct_mrkt_keys", "value": str(len(_all_keys))},
    {"key": "market.in_all_4_datasets",     "value": str(len(_in_all_4))},
    {"key": "market.confirmed_m1",          "value": str(_mkt_confirmed)},
    {"key": "market.pending",               "value": str(_mkt_pending)},
    {"key": "market.m1_coverage_pct",       "value": str(_mkt_cov_pct)},
    {"key": "upc.sap_mat_idt_total",        "value": str(_sap_mat_total)},
    {"key": "upc.sap_ean_coverage_pct",     "value": str(_sap_ean_cov_pct)},
    {"key": "upc.p0_match_rate_pct",        "value": str(_p0_overall_pct)},
    {"key": "upc.p1_match_rate_pct",        "value": str(_p1_rate)},
    {"key": "run.hard_blocker_count",       "value": str(len(_HARD_BLOCKERS))},
    {"key": "run.warning_count",            "value": str(len(_WARNINGS))},
]

df_flat_summary = spark.createDataFrame(pd.DataFrame(_flat_rows))
_flat_dbfs_path = f"{DBFS_BASE}/profiling/summary/catalog_profiling_summary_v6_flat.csv"
save_df(df_flat_summary, _flat_dbfs_path, SECTION)
log("INFO", f"Flat CSV summary written to: {_flat_dbfs_path}", SECTION)

# ----------------------------------------------------------
# 5E: Final validation gate + log flush
# ----------------------------------------------------------
log("INFO", "", SECTION)
log("INFO", "FINAL VALIDATION STATUS:", SECTION)
if _HARD_BLOCKERS:
    log("BLOCKER", f"{len(_HARD_BLOCKERS)} HARD BLOCKERS -- MDM enrichment BLOCKED:", SECTION)
    for _b in _HARD_BLOCKERS:
        log("BLOCKER", f"  {_b}", SECTION)
else:
    passed("No hard blockers. MDM enrichment pipeline is unblocked.", SECTION)

if _WARNINGS:
    log("WARNING", f"{len(_WARNINGS)} warnings -- review before final MDM promotion:", SECTION)
    for _w in _WARNINGS:
        log("WARNING", f"  {_w}", SECTION)
else:
    passed("No warnings.", SECTION)

# Flush all logs
flush_log(f"catalog_profiling_report_v6_{RUN_DATE}.txt")

# Hard raise if blockers (deduplicate before reporting)
_HARD_BLOCKERS = list(dict.fromkeys(_HARD_BLOCKERS))
if _HARD_BLOCKERS:
    raise RuntimeError(
        f"CATALOG PROFILING BLOCKED: {len(_HARD_BLOCKERS)} hard blocker(s).\n"
        + "\n".join(_HARD_BLOCKERS)
    )

print(f"\nOK Catalog profiling complete -- v{CATALOG_VERSION} -- {RUN_DATE}")
print(f"   Hard blockers: {len(_HARD_BLOCKERS)}  |  Warnings: {len(_WARNINGS)}")
print(f"   Summary JSON: {_json_dbfs_path}")
print(f"   Summary CSV : {_flat_dbfs_path}")

# COMMAND ----------


