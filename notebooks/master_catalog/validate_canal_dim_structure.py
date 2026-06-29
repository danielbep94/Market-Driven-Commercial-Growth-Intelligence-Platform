# Databricks notebook source
# MAGIC %md
# MAGIC # Canal Dimension Structure Validation -- v6
# MAGIC
# MAGIC **Purpose:** Validate and profile the commercial channel (CANAL) dimension structure across
# MAGIC all three contributing sources: SELL_IN (V_D_CLIENT), SELL_OUT (VW_D_STORE_RM), and IBP (VW_FACT_DANONE_IBP).
# MAGIC
# MAGIC **Plan reference:** Master Data Catalog Implementation Plan v6 -- Findings C9, C10, C12
# MAGIC
# MAGIC **Key v6 changes driving this notebook:**
# MAGIC - **C9** -- IBP CANAL upgraded from warning to confirmed: full 5-level hierarchy GRAN_CANAL -> CANAL -> GRUPO -> CADENA -> FM
# MAGIC - **C10** -- SELL_IN V_D_CLIENT: new columns lv6_hie_cus_dsc, cus_sal_plt_dsc, stat_cod; column corrected to cus_chl_are_dsc (no _EXT)
# MAGIC - **C12** -- SELL_OUT VW_D_STORE_RM: full store geography discovered (CITY, STATE, REGION, CLUSTER); 3-level hierarchy FORMAT -> CHAIN -> SUBCHAIN
# MAGIC
# MAGIC **Credential resolution:**
# MAGIC - PRD_MEX -> configs/snowflake_creds.py SF_MEX_* (PRD_OSM_DPH_READER)
# MAGIC - PRD_MDP -> SF_MDP_* or Key Vault (DAN-AM-P-KVT800-R-MDP-DB)
# MAGIC
# MAGIC **Output root:** dbfs:/mnt/mdp/mdm/master_catalog/canal/eda_canal_dim_validation
# MAGIC
# MAGIC **Run notebooks/validate_credentials.py first -- all 6 cells must pass.**

# COMMAND ----------

# == CELL 1: Load credentials ==================================================
import os, importlib.util, datetime, math
from pyspark.sql import functions as F

_current_dir = os.getcwd()
_creds_path  = os.path.normpath(os.path.join(_current_dir, "..", "..", "configs", "snowflake_creds.py"))

if not os.path.exists(_creds_path):
    raise FileNotFoundError(
        "configs/snowflake_creds.py NOT FOUND.\n"
        "   Copy configs/snowflake_creds.example.py -> configs/snowflake_creds.py"
    )

_spec = importlib.util.spec_from_file_location("snowflake_creds", _creds_path)
_m    = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_m)

SF_URL     = "danonenam.east-us-2.azure.snowflakecomputing.com"
DB_PRD_MEX = "PRD_MEX"
DB_PRD_MDP = "PRD_MDP"


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
        raise ValueError(f"No profile for '{database}'. Available: {list(profiles.keys())}")
    return dict(profiles[database])


def run_sf(database: str, sql: str):
    """Execute SQL against Snowflake and return a Spark DataFrame."""
    return (spark.read.format("net.snowflake.spark.snowflake")
            .options(**get_sf_options(database))
            .option("sfDatabase", database)
            .option("query", sql)
            .load())


print(f"OK Credentials loaded -- PRD_MEX: {_m.SF_MEX_USER}")

# COMMAND ----------

# == CELL 2: Output paths + helpers ============================================
import pathlib
import pandas as pd

DBFS_ROOT  = "dbfs:/mnt/mdp/mdm/master_catalog/canal/eda_canal_dim_validation"
LOCAL_ROOT = "/dbfs/mnt/mdp/mdm/master_catalog/canal/eda_canal_dim_validation"

# D4: DBFS directory initialisation -- failure is a hard blocker
try:
    dbutils.fs.mkdirs(DBFS_ROOT)
    print(f"OK DBFS root ready: {DBFS_ROOT}")
except Exception as _e:
    raise RuntimeError(
        f"BLOCKER: Could not create DBFS directory {DBFS_ROOT}. Error: {_e}\n"
        "   Verify that dbfs:/mnt/mdp is mounted and the service principal has write access."
    )

_REPO_ROOT    = str(pathlib.Path(_current_dir).parent.parent)
REPO_LOGS_DIR = os.path.join(_REPO_ROOT, "logs", "catalog_eda")
os.makedirs(REPO_LOGS_DIR, exist_ok=True)

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


def flush_log(filename: str = "canal_dim_validation_report.txt"):
    content = "\n".join(_LOG_LINES)
    dbfs_path = f"{DBFS_ROOT}/{filename}"
    dbutils.fs.put(dbfs_path, content, overwrite=True)
    repo_log = os.path.join(REPO_LOGS_DIR, filename)
    with open(repo_log, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"LOG -> DBFS: {dbfs_path}")
    print(f"LOG -> REPO: {repo_log}")


def save_df(df, filename: str, section: str = ""):
    """Write a Spark DataFrame as CSV to DBFS and (pandas) to repo logs."""
    dbfs_path = f"{DBFS_ROOT}/{filename}"
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(dbfs_path)
    fname = os.path.basename(filename)
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


print(f"OK CELL 2 ready. DBFS: {DBFS_ROOT} | REPO: {REPO_LOGS_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1 -- IBP Channel Hierarchy Profiling
# MAGIC
# MAGIC **Source:** PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP
# MAGIC
# MAGIC **Query (C9 confirmed):**
# MAGIC ```sql
# MAGIC SELECT DISTINCT GRAN_CANAL, CANAL, GRUPO, CADENA, FM
# MAGIC FROM PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP
# MAGIC ```
# MAGIC
# MAGIC **Goal:** Profile all 5 IBP channel levels; confirm they are real operational channels.

# COMMAND ----------

SECTION = "S1_IBP_CANAL"
log("INFO", "=" * 70, SECTION)
log("INFO", "IBP CHANNEL HIERARCHY PROFILING -- VW_FACT_DANONE_IBP", SECTION)
log("INFO", "Plan ref: Finding C9 -- IBP CANAL upgraded to confirmed 5-level hierarchy", SECTION)

IBP_CANAL_COLS = ["GRAN_CANAL", "CANAL", "GRUPO", "CADENA", "FM"]

# 1a: Distinct hierarchy combinations (non-null GRAN_CANAL)
log("INFO", "Loading IBP distinct hierarchy combinations...", SECTION)
df_ibp_hier = run_sf(DB_PRD_MDP, """
    SELECT DISTINCT GRAN_CANAL, CANAL, GRUPO, CADENA, FM
    FROM PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP
    WHERE GRAN_CANAL IS NOT NULL
""")
df_ibp_hier.cache()
ibp_hier_count = df_ibp_hier.count()
log("INFO", f"Distinct IBP hierarchy combinations (non-null GRAN_CANAL): {ibp_hier_count:,}", SECTION)

# 1b: Full fact for null profiling
log("INFO", "Loading full IBP channel columns for null/distinct profiling...", SECTION)
df_ibp_full = run_sf(DB_PRD_MDP, """
    SELECT GRAN_CANAL, CANAL, GRUPO, CADENA, FM
    FROM PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP
""")
df_ibp_full.cache()
ibp_total = df_ibp_full.count()
log("INFO", f"Total IBP fact rows (all scenarios): {ibp_total:,}", SECTION)

# 1c: Distinct count and null% per column
ibp_profile_rows = []
for col in IBP_CANAL_COLS:
    null_cnt = df_ibp_full.filter(F.col(col).isNull()).count()
    dist_cnt = df_ibp_full.select(col).distinct().count()
    null_pct = round(null_cnt / ibp_total * 100, 2) if ibp_total > 0 else 0.0
    score    = d1_score(null_pct, dist_cnt)
    ibp_profile_rows.append({
        "source": "IBP", "column": col,
        "total_rows": ibp_total, "null_count": null_cnt,
        "null_pct": null_pct, "distinct_count": dist_cnt, "d1_score": score,
    })
    log("INFO", f"  {col:<15} | distinct={dist_cnt:>5,} | null%={null_pct:>6.2f}% | D1={score:.4f}", SECTION)

df_ibp_profile = spark.createDataFrame(pd.DataFrame(ibp_profile_rows))
save_df(df_ibp_profile, "ibp_canal_column_profile.csv", SECTION)

# 1d: Top 30 GRAN_CANAL
log("INFO", "Top 30 GRAN_CANAL values:", SECTION)
df_gran_canal_top = (df_ibp_full.groupBy("GRAN_CANAL").count()
                     .orderBy(F.desc("count")).limit(30))
df_gran_canal_top.show(30, truncate=False)
save_df(df_gran_canal_top, "ibp_gran_canal_top30.csv", SECTION)

# 1e: Top 30 CANAL
log("INFO", "Top 30 CANAL values:", SECTION)
df_canal_top = (df_ibp_full.groupBy("CANAL").count()
                .orderBy(F.desc("count")).limit(30))
df_canal_top.show(30, truncate=False)
save_df(df_canal_top, "ibp_canal_top30.csv", SECTION)

# 1f: GRAN_CANAL -> CANAL -> GRUPO -> CADENA hierarchy
log("INFO", "GRAN_CANAL -> CANAL -> GRUPO -> CADENA hierarchy (distinct):", SECTION)
df_ibp_hier4 = (df_ibp_full.select("GRAN_CANAL", "CANAL", "GRUPO", "CADENA")
                .distinct()
                .orderBy("GRAN_CANAL", "CANAL", "GRUPO", "CADENA"))
df_ibp_hier4.show(50, truncate=False)
save_df(df_ibp_hier4, "ibp_gran_canal_canal_grupo_cadena_hierarchy.csv", SECTION)

log("INFO", f"Section 1 complete. IBP hierarchy profiled across {len(IBP_CANAL_COLS)} levels.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2 -- SELL_IN V_D_CLIENT Channel Profiling
# MAGIC
# MAGIC **Source:** PRD_MEX.MEX_DSP_OTC.V_D_CLIENT
# MAGIC
# MAGIC **Key corrections (C10):**
# MAGIC - Column name corrected: cus_chl_are_dsc (was CUS_CHL_ARE_DSC_EXT in v5)
# MAGIC - New columns: lv6_hie_cus_dsc (TIPO_CLIENTE), cus_sal_plt_dsc (CEDIS), stat_cod (ESTATUS)
# MAGIC - stat_cod = 'A' filter required for active clients

# COMMAND ----------

SECTION = "S2_SELLIN_CLIENT"
log("INFO", "=" * 70, SECTION)
log("INFO", "SELL_IN V_D_CLIENT CHANNEL PROFILING", SECTION)
log("INFO", "Plan ref: Finding C10 -- new columns + cus_chl_are_dsc name correction", SECTION)

SELL_IN_CANAL_CANDIDATES = [
    "cus_grn_chl_dsc",   # Primary D1 -- maps to IBP GRAN_CANAL
    "lv6_hie_cus_dsc",   # Secondary D1 -- TIPO_CLIENTE (new C10)
    "cus_chl_are_dsc",   # Tertiary -- corrected: was CUS_CHL_ARE_DSC_EXT in v5
]

# 2a: Load all channel-relevant columns
log("INFO", "Loading V_D_CLIENT (all channel-relevant columns)...", SECTION)
df_client_all = run_sf(DB_PRD_MEX, """
    SELECT
        cus_grn_chl_dsc,
        lv6_hie_cus_dsc,
        ptr_1st_cus_dsc,
        cus_chl_are_dsc,
        cus_sal_plt_dsc,
        stat_cod
    FROM PRD_MEX.MEX_DSP_OTC.V_D_CLIENT
""")
df_client_all.cache()
si_total = df_client_all.count()
log("INFO", f"Total V_D_CLIENT rows (all status): {si_total:,}", SECTION)

# 2b: stat_cod distribution
log("INFO", "stat_cod (ESTATUS) distribution:", SECTION)
df_stat = df_client_all.groupBy("stat_cod").count().orderBy(F.desc("count"))
df_stat.show(20, truncate=False)
save_df(df_stat, "sellin_client_stat_cod_distribution.csv", SECTION)

active_count   = df_client_all.filter(F.col("stat_cod") == "A").count()
inactive_count = si_total - active_count
active_pct     = round(active_count / si_total * 100, 2) if si_total > 0 else 0.0
log("INFO", f"Active (stat_cod='A'): {active_count:,} ({active_pct:.1f}%) | Inactive: {inactive_count:,}", SECTION)
warn(active_pct < 50.0,
     f"Less than 50% of V_D_CLIENT rows are active. active%={active_pct:.1f}. Review expected.", SECTION)

# 2c: Active-only subset for D1 profiling
df_client_active = df_client_all.filter(F.col("stat_cod") == "A")
df_client_active.cache()
log("INFO", f"Active client rows for D1 profiling: {active_count:,}", SECTION)

# 2d: D1 scoring for canal candidates (active clients only)
log("INFO", "D1 profiling for CANAL candidate columns (active clients only)...", SECTION)
si_profile_rows = []
for col in SELL_IN_CANAL_CANDIDATES:
    null_cnt = df_client_active.filter(F.col(col).isNull()).count()
    dist_cnt = df_client_active.select(col).distinct().count()
    null_pct = round(null_cnt / active_count * 100, 2) if active_count > 0 else 0.0
    score    = d1_score(null_pct, dist_cnt)
    si_profile_rows.append({
        "source": "SELL_IN", "column": col,
        "total_active": active_count, "null_count": null_cnt,
        "null_pct": null_pct, "distinct_count": dist_cnt, "d1_score": score,
    })
    log("INFO", f"  {col:<25} | distinct={dist_cnt:>5,} | null%={null_pct:>6.2f}% | D1={score:.4f}", SECTION)

# Also profile CEDIS (informational only -- not a canal candidate)
null_cedis = df_client_active.filter(F.col("cus_sal_plt_dsc").isNull()).count()
dist_cedis = df_client_active.select("cus_sal_plt_dsc").distinct().count()
pct_cedis  = round(null_cedis / active_count * 100, 2) if active_count > 0 else 0.0
log("INFO", f"  {'cus_sal_plt_dsc':<25} | distinct={dist_cedis:>5,} | null%={pct_cedis:>6.2f}% | [CEDIS -- internal ref, not canal]", SECTION)

df_si_profile = spark.createDataFrame(pd.DataFrame(si_profile_rows))
save_df(df_si_profile, "sellin_client_canal_d1_profile.csv", SECTION)

# 2e: Top 30 cus_grn_chl_dsc
log("INFO", "Top 30 cus_grn_chl_dsc (active clients):", SECTION)
df_grn_top = (df_client_active.groupBy("cus_grn_chl_dsc").count()
              .orderBy(F.desc("count")).limit(30))
df_grn_top.show(30, truncate=False)
save_df(df_grn_top, "sellin_cus_grn_chl_dsc_top30.csv", SECTION)

# 2f: Top 30 lv6_hie_cus_dsc (TIPO_CLIENTE -- new C10)
log("INFO", "Top 30 lv6_hie_cus_dsc / TIPO_CLIENTE (active clients):", SECTION)
df_lv6_top = (df_client_active.groupBy("lv6_hie_cus_dsc").count()
              .orderBy(F.desc("count")).limit(30))
df_lv6_top.show(30, truncate=False)
save_df(df_lv6_top, "sellin_lv6_hie_cus_dsc_top30.csv", SECTION)

log("INFO", "Section 2 complete.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3 -- SELL_OUT VW_D_STORE_RM Channel + Geography Profiling
# MAGIC
# MAGIC **Source:** PRD_MDP.MDP_DSP.VW_D_STORE_RM
# MAGIC
# MAGIC **Key findings (C12):**
# MAGIC - Channel hierarchy: FORMAT -> CHAIN -> SUBCHAIN (3 levels)
# MAGIC - Store geography: CITY, STATE, REGION, CLUSTER = internal store ref dim (NOT cat_market)

# COMMAND ----------

SECTION = "S3_SELLOUT_STORE"
log("INFO", "=" * 70, SECTION)
log("INFO", "SELL_OUT VW_D_STORE_RM CHANNEL + GEOGRAPHY PROFILING", SECTION)
log("INFO", "Plan ref: C12 -- full store geography; FORMAT->CHAIN->SUBCHAIN hierarchy", SECTION)

log("INFO", "Loading VW_D_STORE_RM...", SECTION)
df_store = run_sf(DB_PRD_MDP, """
    SELECT DISTINCT INT_ID, CHAIN, FORMAT, STORE_NAME,
                    CITY, STATE, REGION, CLUSTER
    FROM PRD_MDP.MDP_DSP.VW_D_STORE_RM
""")
df_store.cache()
store_count = df_store.count()
log("INFO", f"Distinct VW_D_STORE_RM rows: {store_count:,}", SECTION)

# 3a: Channel hierarchy distinct counts
log("INFO", "Channel dimension distinct counts:", SECTION)
for col in ["FORMAT", "CHAIN"]:
    dist_cnt = df_store.select(col).distinct().count()
    null_cnt = df_store.filter(F.col(col).isNull()).count()
    null_pct = round(null_cnt / store_count * 100, 2) if store_count > 0 else 0.0
    score    = d1_score(null_pct, dist_cnt)
    log("INFO", f"  {col:<12} | distinct={dist_cnt:>5,} | null%={null_pct:>6.2f}% | D1={score:.4f}", SECTION)

# 3b: FORMAT -> CHAIN hierarchy
log("INFO", "FORMAT -> CHAIN hierarchy (distinct combinations):", SECTION)
df_format_chain = (df_store.select("FORMAT", "CHAIN")
                   .distinct()
                   .orderBy("FORMAT", "CHAIN"))
df_format_chain.show(60, truncate=False)
save_df(df_format_chain, "sellout_format_chain_hierarchy.csv", SECTION)

# 3c: Top 20 formats
log("INFO", "Top 20 FORMAT values:", SECTION)
df_format_top = (df_store.groupBy("FORMAT").count()
                 .orderBy(F.desc("count")).limit(20))
df_format_top.show(20, truncate=False)
save_df(df_format_top, "sellout_format_top20.csv", SECTION)

# 3d: Store geography (internal ref dim -- NOT cat_market)
log("INFO", "Store geography distinct counts (internal ref dim -- NOT cat_market):", SECTION)
geo_rows = []
for col in ["CITY", "STATE", "REGION", "CLUSTER"]:
    dist_cnt = df_store.select(col).distinct().count()
    null_cnt = df_store.filter(F.col(col).isNull()).count()
    null_pct = round(null_cnt / store_count * 100, 2) if store_count > 0 else 0.0
    geo_rows.append({"column": col, "distinct_count": dist_cnt,
                     "null_count": null_cnt, "null_pct": null_pct})
    log("INFO", f"  {col:<10} | distinct={dist_cnt:>5,} | null%={null_pct:.2f}%  [internal store geo]", SECTION)

df_geo = spark.createDataFrame(pd.DataFrame(geo_rows))
save_df(df_geo, "sellout_store_geo_profile.csv", SECTION)
log("INFO",
    "NOTE: CITY/STATE/REGION/CLUSTER are internal store geo ref dimensions. "
    "They do NOT contribute to cat_market.csv (Nielsen markets only). C12.", SECTION)

log("INFO", "Section 3 complete.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4 -- CROSS-VALIDATION: IBP GRAN_CANAL vs SELL_IN cus_grn_chl_dsc
# MAGIC
# MAGIC **Plan ref:** MC-A14 -- overlap threshold >= 80%
# MAGIC
# MAGIC Validates that IBP and SELL_IN share a common GRAN_CANAL vocabulary.

# COMMAND ----------

SECTION = "S4_XVAL_GRAN_CANAL"
log("INFO", "=" * 70, SECTION)
log("INFO", "CROSS-VALIDATION: IBP GRAN_CANAL vs SELL_IN cus_grn_chl_dsc", SECTION)
log("INFO", "Plan ref: C9, MC-A14 -- overlap target >= 80%", SECTION)

# Load IBP GRAN_CANAL distinct values
log("INFO", "Loading IBP GRAN_CANAL distinct values...", SECTION)
df_ibp_gc = run_sf(DB_PRD_MDP, """
    SELECT DISTINCT UPPER(TRIM(GRAN_CANAL)) AS gran_canal_std
    FROM PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP
    WHERE GRAN_CANAL IS NOT NULL
""")
ibp_gc_set = set(row["GRAN_CANAL_STD"] for row in df_ibp_gc.collect())
log("INFO", f"IBP GRAN_CANAL distinct values: {len(ibp_gc_set)}", SECTION)

# Load SELL_IN cus_grn_chl_dsc distinct values (active only)
log("INFO", "Loading SELL_IN cus_grn_chl_dsc distinct values (active only)...", SECTION)
df_si_gc = run_sf(DB_PRD_MEX, """
    SELECT DISTINCT UPPER(TRIM(cus_grn_chl_dsc)) AS gran_canal_std
    FROM PRD_MEX.MEX_DSP_OTC.V_D_CLIENT
    WHERE cus_grn_chl_dsc IS NOT NULL
      AND stat_cod = 'A'
""")
si_gc_set = set(row["GRAN_CANAL_STD"] for row in df_si_gc.collect())
log("INFO", f"SELL_IN cus_grn_chl_dsc distinct values (active): {len(si_gc_set)}", SECTION)

# Set operations
in_both      = ibp_gc_set & si_gc_set
ibp_only     = ibp_gc_set - si_gc_set
sell_in_only = si_gc_set  - ibp_gc_set
union_size   = len(ibp_gc_set | si_gc_set)
overlap_pct  = round(len(in_both) / union_size * 100, 2) if union_size > 0 else 0.0
align_score  = round(math.log1p(len(in_both)) * overlap_pct / 100, 4)

log("INFO", f"IN_BOTH:      {len(in_both):>4}  values: {sorted(in_both)}", SECTION)
log("INFO", f"IBP_ONLY:     {len(ibp_only):>4}  values: {sorted(ibp_only)}", SECTION)
log("INFO", f"SELL_IN_ONLY: {len(sell_in_only):>4}  values: {sorted(sell_in_only)}", SECTION)
log("INFO", f"Overlap % (Jaccard):  {overlap_pct:.2f}%", SECTION)
log("INFO", f"Alignment score:      {align_score:.4f}", SECTION)

# MC-A14 enforcement
warn(overlap_pct < 80.0,
     f"MC-A14 WARNING: IBP GRAN_CANAL <> SELL_IN cus_grn_chl_dsc overlap = {overlap_pct:.1f}% "
     f"(threshold: 80%). IBP_ONLY={sorted(ibp_only)}. Business review required.", SECTION)
if overlap_pct >= 80.0:
    passed(f"MC-A14 PASS: overlap = {overlap_pct:.1f}% >= 80%.", SECTION)

# Save result
xval_rows = (
    [{"value": v, "set": "IN_BOTH"}      for v in sorted(in_both)]
  + [{"value": v, "set": "IBP_ONLY"}     for v in sorted(ibp_only)]
  + [{"value": v, "set": "SELL_IN_ONLY"} for v in sorted(sell_in_only)]
)
xval_rows.append({"value": f"OVERLAP_PCT={overlap_pct:.2f}%  ALIGN_SCORE={align_score:.4f}", "set": "SUMMARY"})
df_xval_gc = spark.createDataFrame(pd.DataFrame(xval_rows))
save_df(df_xval_gc, "xval_ibp_gran_canal_vs_sellin_cus_grn_chl_dsc.csv", SECTION)

log("INFO", "Section 4 complete.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5 -- CROSS-VALIDATION: IBP CANAL vs SELL_IN lv6_hie_cus_dsc + cus_grn_chl_dsc

# COMMAND ----------

SECTION = "S5_XVAL_CANAL"
log("INFO", "=" * 70, SECTION)
log("INFO", "CROSS-VALIDATION: IBP CANAL vs SELL_IN lv6_hie_cus_dsc + cus_grn_chl_dsc", SECTION)
log("INFO", "Plan ref: C9, C10 -- secondary-level channel alignment", SECTION)

# Load IBP CANAL distinct values
log("INFO", "Loading IBP CANAL distinct values...", SECTION)
df_ibp_canal = run_sf(DB_PRD_MDP, """
    SELECT DISTINCT UPPER(TRIM(CANAL)) AS canal_std
    FROM PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP
    WHERE CANAL IS NOT NULL
""")
ibp_canal_set = set(row["CANAL_STD"] for row in df_ibp_canal.collect())
log("INFO", f"IBP CANAL distinct values: {len(ibp_canal_set)}", SECTION)

# Compare vs lv6_hie_cus_dsc (TIPO_CLIENTE)
log("INFO", "Loading SELL_IN lv6_hie_cus_dsc distinct values (active)...", SECTION)
df_si_lv6 = run_sf(DB_PRD_MEX, """
    SELECT DISTINCT UPPER(TRIM(lv6_hie_cus_dsc)) AS canal_std
    FROM PRD_MEX.MEX_DSP_OTC.V_D_CLIENT
    WHERE lv6_hie_cus_dsc IS NOT NULL
      AND stat_cod = 'A'
""")
si_lv6_set  = set(row["CANAL_STD"] for row in df_si_lv6.collect())
in_both_lv6 = ibp_canal_set & si_lv6_set
union_lv6   = ibp_canal_set | si_lv6_set
overlap_lv6 = round(len(in_both_lv6) / len(union_lv6) * 100, 2) if union_lv6 else 0.0
log("INFO", f"  IBP CANAL <> lv6_hie_cus_dsc overlap: {overlap_lv6:.1f}% ({len(in_both_lv6)} common)", SECTION)
log("INFO", f"  IBP_ONLY: {sorted(ibp_canal_set - si_lv6_set)}", SECTION)
log("INFO", f"  SI_ONLY:  {sorted(si_lv6_set - ibp_canal_set)}", SECTION)

# Compare vs cus_grn_chl_dsc (si_gc_set loaded in Section 4)
in_both_grn = ibp_canal_set & si_gc_set
union_grn   = ibp_canal_set | si_gc_set
overlap_grn = round(len(in_both_grn) / len(union_grn) * 100, 2) if union_grn else 0.0
log("INFO", f"  IBP CANAL <> cus_grn_chl_dsc overlap: {overlap_grn:.1f}% ({len(in_both_grn)} common)", SECTION)

warn(overlap_lv6 < 30.0 and overlap_grn < 30.0,
     f"IBP CANAL low alignment: lv6_hie_cus_dsc={overlap_lv6:.1f}% and "
     f"cus_grn_chl_dsc={overlap_grn:.1f}%. IBP CANAL may be a different hierarchy level. "
     "D1 selection should prefer cus_grn_chl_dsc for GRAN_CANAL alignment.", SECTION)

# Save result
canal_xval_rows = (
    [{"ibp_canal": v, "si_col": "lv6_hie_cus_dsc", "match": v in si_lv6_set} for v in sorted(ibp_canal_set)]
  + [{"ibp_canal": v, "si_col": "cus_grn_chl_dsc", "match": v in si_gc_set}  for v in sorted(ibp_canal_set)]
)
df_xval_canal = spark.createDataFrame(pd.DataFrame(canal_xval_rows))
save_df(df_xval_canal, "xval_ibp_canal_vs_sellin_channels.csv", SECTION)

log("INFO", "Section 5 complete.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6 -- CROSS-VALIDATION: IBP CADENA vs SELL_OUT CHAIN

# COMMAND ----------

SECTION = "S6_XVAL_CADENA_CHAIN"
log("INFO", "=" * 70, SECTION)
log("INFO", "CROSS-VALIDATION: IBP CADENA vs SELL_OUT VW_D_STORE_RM CHAIN", SECTION)
log("INFO", "Plan ref: C9 (IBP CADENA = level 4), C12 (SELL_OUT CHAIN confirmed)", SECTION)

# Load IBP CADENA distinct values
log("INFO", "Loading IBP CADENA distinct values...", SECTION)
df_ibp_cadena = run_sf(DB_PRD_MDP, """
    SELECT DISTINCT UPPER(TRIM(CADENA)) AS cadena_std
    FROM PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP
    WHERE CADENA IS NOT NULL
""")
ibp_cadena_set = set(row["CADENA_STD"] for row in df_ibp_cadena.collect())
log("INFO", f"IBP CADENA distinct values: {len(ibp_cadena_set)}", SECTION)

# Load SELL_OUT CHAIN distinct values
log("INFO", "Loading SELL_OUT VW_D_STORE_RM CHAIN distinct values...", SECTION)
df_so_chain = run_sf(DB_PRD_MDP, """
    SELECT DISTINCT UPPER(TRIM(CHAIN)) AS cadena_std
    FROM PRD_MDP.MDP_DSP.VW_D_STORE_RM
    WHERE CHAIN IS NOT NULL
""")
so_chain_set  = set(row["CADENA_STD"] for row in df_so_chain.collect())
log("INFO", f"SELL_OUT CHAIN distinct values: {len(so_chain_set)}", SECTION)

in_both_cc    = ibp_cadena_set & so_chain_set
ibp_cad_only  = ibp_cadena_set - so_chain_set
so_chain_only = so_chain_set   - ibp_cadena_set
union_cc      = ibp_cadena_set | so_chain_set
match_rate    = round(len(in_both_cc) / len(union_cc) * 100, 2) if union_cc else 0.0

log("INFO", f"IN_BOTH:       {len(in_both_cc):>4}  sample: {sorted(list(in_both_cc))[:10]}", SECTION)
log("INFO", f"IBP_ONLY:      {len(ibp_cad_only):>4}  sample: {sorted(list(ibp_cad_only))[:10]}", SECTION)
log("INFO", f"SELL_OUT_ONLY: {len(so_chain_only):>4}  sample: {sorted(list(so_chain_only))[:10]}", SECTION)
log("INFO", f"Jaccard match rate: {match_rate:.1f}%", SECTION)

warn(match_rate < 40.0,
     f"IBP CADENA <> SELL_OUT CHAIN match rate = {match_rate:.1f}% (below 40%). "
     "Naming conventions may differ. Manual reconciliation recommended.", SECTION)
if match_rate >= 40.0:
    passed(f"IBP CADENA <> SELL_OUT CHAIN match rate = {match_rate:.1f}%.", SECTION)

cc_rows = (
    [{"value": v, "set": "IN_BOTH"}        for v in sorted(in_both_cc)]
  + [{"value": v, "set": "IBP_ONLY"}       for v in sorted(ibp_cad_only)]
  + [{"value": v, "set": "SELL_OUT_ONLY"}  for v in sorted(so_chain_only)]
)
cc_rows.append({"value": f"MATCH_RATE={match_rate:.2f}%", "set": "SUMMARY"})
df_xval_cc = spark.createDataFrame(pd.DataFrame(cc_rows))
save_df(df_xval_cc, "xval_ibp_cadena_vs_sellout_chain.csv", SECTION)

log("INFO", "Section 6 complete.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7 -- D1 WINNER SELECTION for CANAL Catalog
# MAGIC
# MAGIC Combines D1 scores from all three sources (IBP, SELL_IN, SELL_OUT),
# MAGIC selects the best column per source, and writes the final canal seed.

# COMMAND ----------

SECTION = "S7_D1_WINNER"
log("INFO", "=" * 70, SECTION)
log("INFO", "D1 WINNER SELECTION -- CANAL CATALOG", SECTION)

# SELL_OUT D1 scores (FORMAT and CHAIN -- SUBCHAIN not in VW_D_STORE_RM query)
log("INFO", "Computing D1 scores for SELL_OUT FORMAT and CHAIN...", SECTION)
so_d1_rows = []
for col in ["FORMAT", "CHAIN"]:
    null_cnt = df_store.filter(F.col(col).isNull()).count()
    dist_cnt = df_store.select(col).distinct().count()
    null_pct = round(null_cnt / store_count * 100, 2) if store_count > 0 else 0.0
    score    = d1_score(null_pct, dist_cnt)
    so_d1_rows.append({
        "source": "SELL_OUT", "column": col,
        "total_rows": store_count, "null_pct": null_pct,
        "distinct_count": dist_cnt, "d1_score": score,
    })
    log("INFO", f"  {col:<12} | distinct={dist_cnt:>5,} | null%={null_pct:>6.2f}% | D1={score:.4f}", SECTION)

ibp_scores = {r["column"]: r["d1_score"] for r in ibp_profile_rows}
si_scores  = {r["column"]: r["d1_score"] for r in si_profile_rows}
so_scores  = {r["column"]: r["d1_score"] for r in so_d1_rows}


def best_col(scores: dict) -> tuple:
    if not scores:
        return ("N/A", 0.0)
    best = max(scores, key=scores.get)
    return (best, scores[best])


ibp_best_col, ibp_best_score = best_col(ibp_scores)
si_best_col,  si_best_score  = best_col(si_scores)
so_best_col,  so_best_score  = best_col(so_scores)

winner_rows = [
    {"source": "IBP",      "best_column": ibp_best_col, "d1_score": ibp_best_score, "promoted": "YES"},
    {"source": "SELL_IN",  "best_column": si_best_col,  "d1_score": si_best_score,  "promoted": "YES"},
    {"source": "SELL_OUT", "best_column": so_best_col,  "d1_score": so_best_score,  "promoted": "YES"},
]

log("INFO", "=" * 60, SECTION)
log("INFO", "FINAL D1 WINNER TABLE:", SECTION)
log("INFO", f"  {'SOURCE':<12} | {'BEST_COLUMN':<25} | {'D1_SCORE':>8} | PROMOTED", SECTION)
log("INFO", f"  {'-'*12}-+-{'-'*25}-+-{'-'*8}-+---------", SECTION)
for r in winner_rows:
    log("INFO", f"  {r['source']:<12} | {r['best_column']:<25} | {r['d1_score']:>8.4f} | {r['promoted']}", SECTION)
log("INFO", "=" * 60, SECTION)

# Validate against plan expectations
expected = {"IBP": "GRAN_CANAL", "SELL_IN": "cus_grn_chl_dsc", "SELL_OUT": "FORMAT"}
for r in winner_rows:
    exp = expected.get(r["source"])
    if exp and r["best_column"] != exp:
        warn(True,
             f"D1 winner for {r['source']} is '{r['best_column']}' "
             f"but plan expects '{exp}'. Review profiling results.", SECTION)
    elif exp:
        passed(f"D1 winner for {r['source']}: '{r['best_column']}' matches plan expectation.", SECTION)

# Build final canal seed DataFrame
all_d1_rows = ibp_profile_rows + si_profile_rows + so_d1_rows
promoted_set = {ibp_best_col, si_best_col, so_best_col}
for r in all_d1_rows:
    r["promoted"] = "YES" if r.get("column") in promoted_set else "NO"

df_d1_winner = spark.createDataFrame(pd.DataFrame(all_d1_rows))
save_df(df_d1_winner, "canal_d1_winner_table.csv", SECTION)

winner_summary = (
    "CANAL D1 WINNER SELECTION SUMMARY\n"
    + "=" * 50 + "\n"
    + "\n".join(
        f"  {r['source']:<12} | {r['best_column']:<25} | D1={r['d1_score']:.4f} | promoted={r['promoted']}"
        for r in winner_rows
    )
    + f"\n\nGenerated: {ts()}"
)
dbutils.fs.put(f"{DBFS_ROOT}/canal_d1_winner_summary.txt", winner_summary, overwrite=True)
log("INFO", f"Canal winner summary -> {DBFS_ROOT}/canal_d1_winner_summary.txt", SECTION)
log("INFO", "Section 7 complete.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 8 -- SUMMARY REPORT

# COMMAND ----------

SECTION = "S8_SUMMARY"
log("INFO", "=" * 70, SECTION)
log("INFO", "CANAL DIM VALIDATION -- SUMMARY REPORT", SECTION)
log("INFO", f"Generated: {ts()}", SECTION)
log("INFO", "", SECTION)

log("INFO", "SOURCE PROFILE SUMMARY:", SECTION)
log("INFO", f"  IBP VW_FACT_DANONE_IBP   -- {ibp_total:>8,} total rows | {ibp_hier_count:>5,} distinct hierarchy combos", SECTION)
log("INFO", f"  SELL_IN V_D_CLIENT        -- {si_total:>8,} total rows | {active_count:>5,} active (stat_cod='A')", SECTION)
log("INFO", f"  SELL_OUT VW_D_STORE_RM    -- {store_count:>8,} distinct stores", SECTION)
log("INFO", "", SECTION)

log("INFO", "CROSS-VALIDATION RESULTS:", SECTION)
log("INFO", f"  [S4] IBP GRAN_CANAL <> SELL_IN cus_grn_chl_dsc -- overlap={overlap_pct:.1f}%  align_score={align_score:.4f}", SECTION)
log("INFO", f"  [S5] IBP CANAL      <> SELL_IN lv6_hie_cus_dsc -- overlap={overlap_lv6:.1f}%", SECTION)
log("INFO", f"  [S5] IBP CANAL      <> SELL_IN cus_grn_chl_dsc -- overlap={overlap_grn:.1f}%", SECTION)
log("INFO", f"  [S6] IBP CADENA     <> SELL_OUT CHAIN           -- match_rate={match_rate:.1f}%", SECTION)
log("INFO", "", SECTION)

log("INFO", "D1 WINNERS (promoted=YES):", SECTION)
for r in winner_rows:
    log("INFO", f"  {r['source']:<12} -> {r['best_column']:<25}  D1={r['d1_score']:.4f}", SECTION)
log("INFO", "", SECTION)

log("INFO", "VALIDATION ASSERTIONS:", SECTION)
if _HARD_BLOCKERS:
    log("BLOCKER", f"{len(_HARD_BLOCKERS)} HARD BLOCKERS -- catalog build must not proceed:", SECTION)
    for b in _HARD_BLOCKERS:
        log("BLOCKER", f"  {b}", SECTION)
else:
    passed("No hard blockers found.", SECTION)

if _WARNINGS:
    log("WARNING", f"{len(_WARNINGS)} warnings -- review before promoting to cat_canal.csv:", SECTION)
    for w in _WARNINGS:
        log("WARNING", f"  {w}", SECTION)
else:
    passed("No warnings.", SECTION)

log("INFO", "", SECTION)
log("INFO", "DBFS OUTPUT FILES:", SECTION)
for fname in [
    "ibp_canal_column_profile.csv",
    "ibp_gran_canal_top30.csv",
    "ibp_canal_top30.csv",
    "ibp_gran_canal_canal_grupo_cadena_hierarchy.csv",
    "sellin_client_stat_cod_distribution.csv",
    "sellin_client_canal_d1_profile.csv",
    "sellin_cus_grn_chl_dsc_top30.csv",
    "sellin_lv6_hie_cus_dsc_top30.csv",
    "sellout_format_chain_hierarchy.csv",
    "sellout_format_top20.csv",
    "sellout_store_geo_profile.csv",
    "xval_ibp_gran_canal_vs_sellin_cus_grn_chl_dsc.csv",
    "xval_ibp_canal_vs_sellin_channels.csv",
    "xval_ibp_cadena_vs_sellout_chain.csv",
    "canal_d1_winner_table.csv",
    "canal_d1_winner_summary.txt",
    "canal_dim_validation_report.txt",
]:
    log("INFO", f"  {DBFS_ROOT}/{fname}", SECTION)

# Flush log to DBFS + repo
flush_log("canal_dim_validation_report.txt")

# Final assert: raise if any hard blockers
if _HARD_BLOCKERS:
    raise RuntimeError(
        f"CANAL DIM VALIDATION FAILED: {len(_HARD_BLOCKERS)} hard blocker(s).\n"
        + "\n".join(_HARD_BLOCKERS)
    )

print(f"OK Canal Dim Validation complete -- {len(_WARNINGS)} warnings, 0 blockers.")
print(f"   Report: {DBFS_ROOT}/canal_dim_validation_report.txt")

# COMMAND ----------


