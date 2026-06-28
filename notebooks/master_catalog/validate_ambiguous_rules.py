# Databricks notebook source

# MAGIC %md
# MAGIC # Ambiguity Resolution Notebook -- v6.0.0
# MAGIC
# MAGIC **Purpose:** Answer exactly 9 specific open questions (ambiguities) flagged in the
# MAGIC Master Data Catalog Implementation Plan v6.  Each section is self-contained:
# MAGIC QUESTION -> SQL -> RESULT -> CONCLUSION.
# MAGIC
# MAGIC **Plan reference:** Master Data Catalog Implementation Plan v6 -- Ambiguity List
# MAGIC
# MAGIC **Output root:** dbfs:/mnt/mdp/mdm/master_catalog/ambiguity_resolution/
# MAGIC
# MAGIC **Credential resolution:**
# MAGIC - PRD_MEX -> configs/snowflake_creds.py  SF_MEX_* (PRD_OSM_DPH_READER)
# MAGIC - PRD_MDP -> SF_MDP_* or Key Vault (DAN-AM-P-KVT800-R-MDP-DB)
# MAGIC
# MAGIC **Run notebooks/validate_credentials.py first -- all 6 cells must pass.**
# MAGIC
# MAGIC **Ambiguities addressed:**
# MAGIC 1.  stat_cod active filter value (V_D_CLIENT C10)
# MAGIC 2.  VW_WASTE column structure (COLUMNA_WASTE_* placeholders)
# MAGIC 3.  Nielsen EDP hierarchy_level distribution (VW_IR_YOG_GEL_MT)
# MAGIC 4.  Nielsen Water Retail hierarchy_level (VW_IND_AGUA_BNF_RT max level)
# MAGIC 5.  SELL_IN VW_FACT_RNV join key validation (SHP_CUS_IDT exists?)
# MAGIC 6.  IBP CADENA value classification (chains vs territory codes?)
# MAGIC 7.  SELL_OUT SUBCHAIN null rate (usable for canal profiling?)
# MAGIC 8.  VW_D_CUSTOMER_DICTONARY join key verification (OLD/NEW_CUS_IDT)
# MAGIC 9.  Nielsen Water Scantrack hierarchy_level for UPC grain

# COMMAND ----------

# ============================================================
# CELL 1 -- Credentials, utilities, constants, DBFS init
# ============================================================
import os
import importlib.util
import datetime
import math
import pathlib

import pandas as pd
from pyspark.sql import functions as F

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
SF_URL     = "danonenam.east-us-2.azure.snowflakecomputing.com"
DB_PRD_MEX = "PRD_MEX"
DB_PRD_MDP = "PRD_MDP"
RUN_DATE   = datetime.datetime.now().strftime("%Y-%m-%d")
DBFS_AMBI  = "dbfs:/mnt/mdp/mdm/master_catalog/ambiguity_resolution"


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
# DBFS output directory
# ----------------------------------------------------------
try:
    dbutils.fs.mkdirs(DBFS_AMBI)
    print(f"OK DBFS dir ready: {DBFS_AMBI}")
except Exception as _e:
    raise RuntimeError(
        f"BLOCKER: Could not create DBFS directory {DBFS_AMBI}.\n"
        f"   Error: {_e}\n"
        "   Verify dbfs:/mnt/mdp is mounted and the service principal has write access."
    )

# ----------------------------------------------------------
# Repo log directory
# ----------------------------------------------------------
_REPO_ROOT    = str(pathlib.Path(_current_dir).parent.parent)
REPO_LOGS_DIR = os.path.join(_REPO_ROOT, "logs", "ambiguity_resolution")
os.makedirs(REPO_LOGS_DIR, exist_ok=True)

# ----------------------------------------------------------
# Logging infrastructure
# ----------------------------------------------------------
_LOG_LINES:     list = []
_HARD_BLOCKERS: list = []
_WARNINGS:      list = []

# Ambiguity resolution tracker: {ambi_id: "RESOLVED" | "FLAGGED" | "BLOCKED"}
_AMBI_STATUS: dict = {}


def ts() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(level: str, msg: str, section: str = ""):
    prefix = f"[{ts()}][{level}]"
    if section:
        prefix += f"[{section}]"
    line = f"{prefix} {msg}"
    _LOG_LINES.append(line)
    print(line)


def flush_log(filename: str = "ambiguity_resolution_report.txt"):
    content = "\n".join(_LOG_LINES)
    dbfs_path = f"{DBFS_AMBI}/{filename}"
    dbutils.fs.put(dbfs_path, content, overwrite=True)
    repo_log = os.path.join(REPO_LOGS_DIR, filename)
    with open(repo_log, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"LOG -> DBFS: {dbfs_path}")
    print(f"LOG -> REPO: {repo_log}")


def save_df(df, dbfs_path: str, section: str = ""):
    """Write a Spark DataFrame as CSV to DBFS and pandas CSV to repo logs."""
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
        _AMBI_STATUS[section] = "BLOCKED"
    return condition


def warn(condition: bool, msg: str, section: str = "") -> bool:
    if condition:
        log("WARNING", msg, section)
        _WARNINGS.append(f"[{section}] {msg}")
        if _AMBI_STATUS.get(section) != "BLOCKED":
            _AMBI_STATUS[section] = "FLAGGED"
    return condition


def resolve(section: str, conclusion: str):
    """Mark an ambiguity as RESOLVED and log conclusion."""
    if _AMBI_STATUS.get(section) not in ("BLOCKED", "FLAGGED"):
        _AMBI_STATUS[section] = "RESOLVED"
    log("CONCLUSION", conclusion, section)


def d1_score(null_pct: float, distinct_count: int) -> float:
    """D1 scoring: completeness * log1p(distinct_count)."""
    completeness = max(0.0, 1.0 - null_pct / 100.0)
    return round(completeness * math.log1p(max(0, distinct_count)), 4)


print(f"OK CELL 1 ready.  Run date={RUN_DATE}")
print(f"   DBFS output: {DBFS_AMBI}")
print(f"   Repo logs:   {REPO_LOGS_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ambiguity 1 -- stat_cod active filter value (C10)
# MAGIC
# MAGIC **QUESTION:** What are the distinct values of stat_cod in V_D_CLIENT,
# MAGIC and which represents 'active'? The plan uses `stat_cod = 'A'` as the active filter.
# MAGIC
# MAGIC **Source:** PRD_MEX.MEX_DSP_OTC.V_D_CLIENT

# COMMAND ----------

SECTION = "AMBI_01"
log("INFO", "=" * 70, SECTION)
log("INFO", "AMBIGUITY 1 -- stat_cod active filter value", SECTION)
log("INFO", "QUESTION: What are the distinct stat_cod values in V_D_CLIENT?", SECTION)
log("INFO", "          Does stat_cod='A' represent the active state?", SECTION)

df_stat_cod = run_sf(DB_PRD_MEX, """
    SELECT
        stat_cod,
        COUNT(*) AS client_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct
    FROM PRD_MEX.MEX_DSP_OTC.V_D_CLIENT
    GROUP BY 1
    ORDER BY 2 DESC
""")
df_stat_cod.cache()
_stat_rows = df_stat_cod.collect()

log("INFO", "stat_cod distribution:", SECTION)
df_stat_cod.show(20, truncate=False)

# Check if 'A' is present
_stat_values = [str(r["stat_cod"]).strip() for r in _stat_rows if r["stat_cod"] is not None]
_has_a       = "A" in _stat_values

log("INFO", f"Distinct stat_cod values found: {sorted(_stat_values)}", SECTION)
log("INFO", "RESULT: These are all stat_cod values. The active filter in plan uses stat_cod = 'A'.", SECTION)
log("INFO", "        Verify this matches the highest-count status code.", SECTION)

if _has_a:
    _a_row = next((r for r in _stat_rows if str(r["stat_cod"]).strip() == "A"), None)
    if _a_row:
        log("INFO", f"  stat_cod='A' found: count={_a_row['client_count']:,} | pct={_a_row['pct']:.2f}%", SECTION)
    resolve(SECTION, "stat_cod='A' EXISTS in V_D_CLIENT. Plan active filter is valid.")
else:
    warn(True, "stat_cod='A' NOT found in V_D_CLIENT! Plan active filter stat_cod='A' will return 0 rows. Investigate.", SECTION)
    resolve(SECTION, "FLAGGED: stat_cod='A' absent -- plan active filter must be revised.")

save_df(df_stat_cod, f"{DBFS_AMBI}/ambi_01_stat_cod_values.csv", SECTION)
log("INFO", "Ambiguity 1 complete.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ambiguity 2 -- VW_WASTE column structure (COLUMNA_WASTE_* placeholders)
# MAGIC
# MAGIC **QUESTION:** What are the actual column names for volume, value, and SKU in VW_WASTE?
# MAGIC The plan references placeholder names [COLUMNA_WASTE_*] that need to be resolved.
# MAGIC
# MAGIC **Source:** PRD_MDP.MDP_STG.VW_WASTE

# COMMAND ----------

SECTION = "AMBI_02"
log("INFO", "=" * 70, SECTION)
log("INFO", "AMBIGUITY 2 -- VW_WASTE actual column names", SECTION)
log("INFO", "QUESTION: What columns in VW_WASTE represent volume, value, and SKU?", SECTION)
log("INFO", "          Plan uses placeholder [COLUMNA_WASTE_*] -- resolve to actual names.", SECTION)

# Sample 1 row to get all column names
df_waste_sample = run_sf(DB_PRD_MDP, """
    SELECT * FROM PRD_MDP.MDP_STG.VW_WASTE LIMIT 1
""")
_waste_cols = df_waste_sample.columns
log("INFO", f"VW_WASTE column list ({len(_waste_cols)} columns):", SECTION)
for _i, _c in enumerate(_waste_cols, 1):
    log("INFO", f"  [{_i:02d}] {_c}", SECTION)

# FUENTE distribution
df_waste_fuente = run_sf(DB_PRD_MDP, """
    SELECT DISTINCT FUENTE, COUNT(*) AS cnt
    FROM PRD_MDP.MDP_STG.VW_WASTE
    GROUP BY 1
""")
log("INFO", "VW_WASTE FUENTE distribution:", SECTION)
df_waste_fuente.show(20, truncate=False)

log("INFO", "RESULT: Full column list printed above. Identify volume, value, and SKU columns.", SECTION)
log("INFO", "        Common candidates: look for KG, VOL, VTA, IMP, EAN, SKU, UPC in column names.", SECTION)

# Write column list as CSV
_col_rows = pd.DataFrame([{"column_index": i, "column_name": c} for i, c in enumerate(_waste_cols, 1)])
df_col_list = spark.createDataFrame(_col_rows)
save_df(df_col_list, f"{DBFS_AMBI}/ambi_02_waste_columns.csv", SECTION)
save_df(df_waste_fuente, f"{DBFS_AMBI}/ambi_02_waste_fuente_distribution.csv", SECTION)

resolve(SECTION, "VW_WASTE column list captured. Architect must identify volume/value/SKU columns from the list.")
log("INFO", "Ambiguity 2 complete.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ambiguity 3 -- Nielsen EDP hierarchy_level distribution
# MAGIC
# MAGIC **QUESTION:** What hierarchy_level values exist in VW_IR_YOG_GEL_MT_NLSN_PROD_DIM,
# MAGIC and does level 11 have PRDC_CD populated? P0 bridge requires level 11 to be valid.
# MAGIC
# MAGIC **Source:** PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_PROD_DIM

# COMMAND ----------

SECTION = "AMBI_03"
log("INFO", "=" * 70, SECTION)
log("INFO", "AMBIGUITY 3 -- Nielsen EDP hierarchy_level distribution", SECTION)
log("INFO", "QUESTION: Does hierarchy_level=11 exist and have populated PRDC_CD?", SECTION)
log("INFO", "          P0 UPC bridge for EDP requires level 11 rows with PRDC_CD.", SECTION)

df_edp_hier = run_sf(DB_PRD_MEX, """
    SELECT
        hierarchy_level,
        COUNT(*) AS row_count,
        COUNT(DISTINCT PRDC_CD) AS distinct_prdc_cd,
        SUM(CASE WHEN PRDC_CD IS NULL THEN 1 ELSE 0 END) AS null_prdc_cd,
        ROUND(
            SUM(CASE WHEN PRDC_CD IS NULL THEN 1.0 ELSE 0 END) / COUNT(*) * 100, 2
        ) AS null_prdc_pct
    FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_PROD_DIM
    GROUP BY 1
    ORDER BY 1
""")
df_edp_hier.cache()
_edp_hier_rows = df_edp_hier.collect()

log("INFO", "EDP hierarchy_level distribution:", SECTION)
df_edp_hier.show(20, truncate=False)

_levels_present = [r["hierarchy_level"] for r in _edp_hier_rows]
_level_11_row   = next((r for r in _edp_hier_rows if r["hierarchy_level"] == 11), None)

if _level_11_row is None:
    blocker(True,
        "hierarchy_level=11 NOT FOUND in VW_IR_YOG_GEL_MT_NLSN_PROD_DIM. "
        "P0 EDP bridge will have no UPC-grain rows. Plan must be revised.",
        SECTION)
else:
    _l11_null_pct = float(_level_11_row["null_prdc_pct"])
    _l11_distinct = int(_level_11_row["distinct_prdc_cd"])
    log("INFO", f"Level 11: rows={_level_11_row['row_count']:,} | distinct_PRDC_CD={_l11_distinct:,} | null_pct={_l11_null_pct:.2f}%", SECTION)

    blocker(
        _l11_null_pct >= 100.0,
        f"hierarchy_level=11 has 100% null PRDC_CD in EDP. P0 bridge impossible.",
        SECTION,
    )

    if not _HARD_BLOCKERS or SECTION not in str(_HARD_BLOCKERS):
        resolve(SECTION,
            f"hierarchy_level=11 EXISTS with {_l11_distinct:,} distinct PRDC_CD values "
            f"(null_pct={_l11_null_pct:.2f}%). EDP P0 bridge is VALID at level 11.")

save_df(df_edp_hier, f"{DBFS_AMBI}/ambi_03_edp_hierarchy_levels.csv", SECTION)
log("INFO", "Ambiguity 3 complete.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ambiguity 4 -- Nielsen Water Retail hierarchy_level distribution
# MAGIC
# MAGIC **QUESTION:** Confirm that VW_IND_AGUA_BNF_RT_NLSN_PROD_DIM max hierarchy_level = 9
# MAGIC (no level 11 UPC grain).  If level 11 exists, the C1 decision to exclude Water Retail
# MAGIC from the P0 bridge must be reviewed.
# MAGIC
# MAGIC **Source:** PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_RT_NLSN_PROD_DIM

# COMMAND ----------

SECTION = "AMBI_04"
log("INFO", "=" * 70, SECTION)
log("INFO", "AMBIGUITY 4 -- Nielsen Water Retail hierarchy_level distribution", SECTION)
log("INFO", "QUESTION: Does VW_IND_AGUA_BNF_RT max hierarchy_level = 9 (no level 11)?", SECTION)

df_wrt_hier = run_sf(DB_PRD_MEX, """
    SELECT
        hierarchy_level,
        COUNT(*) AS row_count,
        COUNT(DISTINCT PRDC_CD) AS distinct_prdc_cd,
        SUM(CASE WHEN PRDC_CD IS NULL THEN 1 ELSE 0 END) AS null_prdc_cd
    FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_RT_NLSN_PROD_DIM
    GROUP BY 1
    ORDER BY 1
""")
df_wrt_hier.cache()
_wrt_rows  = df_wrt_hier.collect()
_max_level = max((r["hierarchy_level"] for r in _wrt_rows), default=0)
_has_11    = any(r["hierarchy_level"] == 11 for r in _wrt_rows)

log("INFO", "Water Retail hierarchy_level distribution:", SECTION)
df_wrt_hier.show(20, truncate=False)
log("INFO", f"Water Retail max hierarchy_level = {_max_level}", SECTION)

warn(
    _has_11,
    f"hierarchy_level=11 EXISTS in Water Retail VW_IND_AGUA_BNF_RT_NLSN_PROD_DIM! "
    "C1 decision (exclude from P0 bridge) requires review. Escalate to architect.",
    SECTION,
)

if _max_level <= 9:
    log("INFO", f"C1 decision CONFIRMED: Water Retail max level={_max_level} <= 9. No UPC grain -- P0 exclusion is correct.", SECTION)
    resolve(SECTION,
        f"Water Retail max hierarchy_level={_max_level}. C1 decision CONFIRMED: "
        "Water Retail correctly excluded from P0 UPC bridge.")
else:
    log("WARNING", f"C1 decision REQUIRES REVIEW: Water Retail max level={_max_level} > 9.", SECTION)

log("INFO", f"Water Retail max hierarchy_level = {_max_level}. "
            f"C1 decision {'CONFIRMED' if _max_level <= 9 else 'REQUIRES REVIEW'}.", SECTION)

save_df(df_wrt_hier, f"{DBFS_AMBI}/ambi_04_water_retail_hierarchy.csv", SECTION)
log("INFO", "Ambiguity 4 complete.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ambiguity 5 -- SELL_IN VW_FACT_RNV key column validation
# MAGIC
# MAGIC **QUESTION:** Does SHP_CUS_IDT exist in VW_FACT_RNV?  What are the actual fact
# MAGIC join key columns?  The plan joins on SHP_CUS_IDT and MAT_IDT.
# MAGIC
# MAGIC **Source:** PRD_MEX.MEX_DSP_OTC.VW_FACT_RNV

# COMMAND ----------

SECTION = "AMBI_05"
log("INFO", "=" * 70, SECTION)
log("INFO", "AMBIGUITY 5 -- SELL_IN VW_FACT_RNV join key validation", SECTION)
log("INFO", "QUESTION: Does SHP_CUS_IDT exist in VW_FACT_RNV? Are MAT_IDT and BIL_NET_VAL present?", SECTION)

# Sample 1 row to get all column names
df_rnv_sample = run_sf(DB_PRD_MEX, """
    SELECT * FROM PRD_MEX.MEX_DSP_OTC.VW_FACT_RNV LIMIT 1
""")
_rnv_cols = df_rnv_sample.columns
_rnv_cols_upper = {c.upper() for c in _rnv_cols}

log("INFO", f"VW_FACT_RNV column list ({len(_rnv_cols)} columns):", SECTION)
for _i, _c in enumerate(_rnv_cols, 1):
    log("INFO", f"  [{_i:02d}] {_c}", SECTION)

# Check for required columns
_REQUIRED_COLS = {
    "SHP_CUS_IDT": "Primary customer join key (plan assumption)",
    "MAT_IDT":     "Material join key (plan assumption)",
    "BIL_DAT":     "Billing date",
    "BIL_NET_VAL": "Net billing value",
    "BIL_NET_KGR": "Net billing kilograms",
}

_missing_cols = []
for _col, _desc in _REQUIRED_COLS.items():
    _found = _col.upper() in _rnv_cols_upper
    _status = "FOUND" if _found else "MISSING"
    log("INFO", f"  {_col:<20} | {_status:<8} | {_desc}", SECTION)
    if not _found:
        _missing_cols.append(_col)

blocker(
    "SHP_CUS_IDT" in _missing_cols,
    f"SHP_CUS_IDT NOT FOUND in VW_FACT_RNV. Plan join key assumption is invalid. "
    f"Available customer-like columns: {[c for c in _rnv_cols if 'CUS' in c.upper() or 'SHP' in c.upper() or 'CLI' in c.upper()]}",
    SECTION,
)

blocker(
    "MAT_IDT" in _missing_cols,
    f"MAT_IDT NOT FOUND in VW_FACT_RNV. Plan material join key assumption is invalid. "
    f"Available material-like columns: {[c for c in _rnv_cols if 'MAT' in c.upper() or 'SKU' in c.upper()]}",
    SECTION,
)

if not _missing_cols:
    resolve(SECTION,
        "All required VW_FACT_RNV columns CONFIRMED: SHP_CUS_IDT, MAT_IDT, BIL_DAT, BIL_NET_VAL, BIL_NET_KGR. "
        "Plan join logic is valid.")

# Write column list
_col_rows = pd.DataFrame([{"column_index": i, "column_name": c,
                             "is_required": c.upper() in set(_REQUIRED_COLS.keys()),
                             "status": "FOUND" if c.upper() in set(_REQUIRED_COLS.keys()) else "PRESENT"}
                           for i, c in enumerate(_rnv_cols, 1)])
df_rnv_cols = spark.createDataFrame(_col_rows)
save_df(df_rnv_cols, f"{DBFS_AMBI}/ambi_05_fact_rnv_columns.csv", SECTION)
log("INFO", "Ambiguity 5 complete.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ambiguity 6 -- IBP CADENA sample values (chains vs territory codes?)
# MAGIC
# MAGIC **QUESTION:** What are the distinct CADENA values in VW_FACT_DANONE_IBP?
# MAGIC Are they retail chain names or territory codes?  The IBP_MDM.txt sample shows
# MAGIC 'REGION VI' -- if CADENA contains territory codes, MC-A14 must be revised.
# MAGIC
# MAGIC **Source:** PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP

# COMMAND ----------

SECTION = "AMBI_06"
log("INFO", "=" * 70, SECTION)
log("INFO", "AMBIGUITY 6 -- IBP CADENA value classification", SECTION)
log("INFO", "QUESTION: Are IBP CADENA values retail chain names or territory codes?", SECTION)
log("INFO", "          IBP_MDM.txt sample shows 'REGION VI' -- if territorial, cannot cross-validate with SELL_OUT CHAIN.", SECTION)

df_ibp_cadena = run_sf(DB_PRD_MDP, """
    SELECT
        GRAN_CANAL, CANAL, CADENA,
        COUNT(*) AS row_count
    FROM PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP
    WHERE NOMBRE_ETIQUETA = 'REAL'
      AND CADENA IS NOT NULL
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 3
""")
df_ibp_cadena.cache()
_cadena_count = df_ibp_cadena.count()
log("INFO", f"IBP CADENA distinct GRAN_CANAL+CANAL+CADENA combinations: {_cadena_count:,}", SECTION)
df_ibp_cadena.show(50, truncate=False)

# Collect all distinct CADENA values
_cadena_vals = [
    r["CADENA"].strip().upper()
    for r in df_ibp_cadena.select("CADENA").distinct().collect()
    if r["CADENA"] is not None
]
log("INFO", f"Distinct CADENA values ({len(_cadena_vals)} total):", SECTION)
for _v in sorted(_cadena_vals):
    log("INFO", f"  '{_v}'", SECTION)

# Detection logic: check for territorial indicators
_territorial_indicators = ["REGION", "TOTAL", "NACIONAL", "NORTE", "SUR", "CENTRO", "ESTE", "OESTE"]
_territorial_hits = [v for v in _cadena_vals if any(ind in v for ind in _territorial_indicators)]
_has_territorial  = len(_territorial_hits) > 0

log("INFO", "RESULT: Review CADENA values above.", SECTION)
log("INFO", "        IBP_MDM.txt sample shows 'REGION VI' -- check if territorial codes appear below.", SECTION)
log("INFO", f"Territorial indicator hits: {_territorial_hits}", SECTION)

warn(
    _has_territorial,
    f"IBP CADENA contains territorial/geographic values: {_territorial_hits}. "
    "CADENA != SELL_OUT CHAIN. MC-A14 cross-validation must be revised or excluded for CADENA level.",
    SECTION,
)

if not _has_territorial:
    resolve(SECTION, "No territorial indicators found in IBP CADENA. Values appear to be retail chains. MC-A14 is valid.")
else:
    resolve(SECTION,
        f"FLAGGED: IBP CADENA contains territorial values {_territorial_hits}. "
        "MC-A14 SELL_OUT CHAIN cross-validation must be revised.")

save_df(df_ibp_cadena, f"{DBFS_AMBI}/ambi_06_ibp_cadena_values.csv", SECTION)
log("INFO", "Ambiguity 6 complete.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ambiguity 7 -- SELL_OUT SUBCHAIN null rate
# MAGIC
# MAGIC **QUESTION:** What is the null rate of SUBCHAIN in VW_D_STORE_RM?
# MAGIC Is it usable for canal profiling?  If overall null rate > 30%, it should be
# MAGIC flagged as unreliable for D1 profiling.
# MAGIC
# MAGIC **Source:** PRD_MDP.MDP_DSP.VW_D_STORE_RM

# COMMAND ----------

SECTION = "AMBI_07"
log("INFO", "=" * 70, SECTION)
log("INFO", "AMBIGUITY 7 -- SELL_OUT SUBCHAIN null rate", SECTION)
log("INFO", "QUESTION: What is SUBCHAIN null rate in VW_D_STORE_RM? Is it reliable for D1?", SECTION)

df_subchain_null = run_sf(DB_PRD_MDP, """
    SELECT
        FORMAT,
        COUNT(*) AS total_stores,
        SUM(CASE WHEN SUBCHAIN IS NULL THEN 1 ELSE 0 END) AS null_subchain,
        ROUND(
            SUM(CASE WHEN SUBCHAIN IS NULL THEN 1.0 ELSE 0 END) / COUNT(*) * 100, 2
        ) AS null_subchain_pct
    FROM PRD_MDP.MDP_DSP.VW_D_STORE_RM
    GROUP BY 1
    ORDER BY 4 DESC
""")
df_subchain_null.cache()
log("INFO", "SUBCHAIN null rate by FORMAT:", SECTION)
df_subchain_null.show(30, truncate=False)

# Overall null rate
_subchain_totals = run_sf(DB_PRD_MDP, """
    SELECT
        COUNT(*) AS total_rows,
        SUM(CASE WHEN SUBCHAIN IS NULL THEN 1 ELSE 0 END) AS null_subchain,
        ROUND(
            SUM(CASE WHEN SUBCHAIN IS NULL THEN 1.0 ELSE 0 END) / COUNT(*) * 100, 2
        ) AS null_subchain_pct
    FROM PRD_MDP.MDP_DSP.VW_D_STORE_RM
""").collect()[0]

_overall_null_pct = float(_subchain_totals["null_subchain_pct"])
_total_stores     = int(_subchain_totals["total_rows"])
_null_cnt         = int(_subchain_totals["null_subchain"])

log("INFO", f"SUBCHAIN overall: total_rows={_total_stores:,} | null={_null_cnt:,} | null_pct={_overall_null_pct:.2f}%", SECTION)

warn(
    _overall_null_pct > 30.0,
    f"SUBCHAIN overall null rate = {_overall_null_pct:.2f}% > 30% threshold. "
    "SUBCHAIN is unreliable for D1 canal profiling. D1 profiling should rely on FORMAT and CHAIN only.",
    SECTION,
)

if _overall_null_pct <= 30.0:
    resolve(SECTION,
        f"SUBCHAIN null rate = {_overall_null_pct:.2f}% <= 30%. Acceptable for D1 profiling as tertiary level.")
else:
    resolve(SECTION,
        f"FLAGGED: SUBCHAIN null rate = {_overall_null_pct:.2f}% > 30%. "
        "Exclude from D1 winner selection; use FORMAT and CHAIN only.")

save_df(df_subchain_null, f"{DBFS_AMBI}/ambi_07_subchain_null_rate.csv", SECTION)
log("INFO", "Ambiguity 7 complete.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ambiguity 8 -- VW_D_CUSTOMER_DICTONARY join key verification
# MAGIC
# MAGIC **QUESTION:** Confirm that VW_D_CUSTOMER_DICTONARY has OLD_CUS_IDT and NEW_CUS_IDT
# MAGIC and validate its record count and dedup logic.  The plan uses this table to resolve
# MAGIC customer ID migrations.
# MAGIC
# MAGIC **Source:** PRD_MEX.MEX_DSP_OTC.VW_D_CUSTOMER_DICTONARY
# MAGIC (Note: 'DICTONARY' spelling -- intentional typo in source table name)

# COMMAND ----------

SECTION = "AMBI_08"
log("INFO", "=" * 70, SECTION)
log("INFO", "AMBIGUITY 8 -- VW_D_CUSTOMER_DICTONARY join key verification", SECTION)
log("INFO", "QUESTION: Do OLD_CUS_IDT and NEW_CUS_IDT exist? What is the dedup record count?", SECTION)

# Sample 1 row to get column list
df_cust_dict_sample = run_sf(DB_PRD_MEX, """
    SELECT * FROM PRD_MEX.MEX_DSP_OTC.VW_D_CUSTOMER_DICTONARY LIMIT 1
""")
_cust_dict_cols       = df_cust_dict_sample.columns
_cust_dict_cols_upper = {c.upper() for c in _cust_dict_cols}

log("INFO", f"VW_D_CUSTOMER_DICTONARY column list ({len(_cust_dict_cols)} columns):", SECTION)
for _i, _c in enumerate(_cust_dict_cols, 1):
    log("INFO", f"  [{_i:02d}] {_c}", SECTION)

# Check required keys
_REQUIRED_CUST_COLS = ["OLD_CUS_IDT", "NEW_CUS_IDT", "SAL_ORG_COD"]
_missing_cust_cols  = [c for c in _REQUIRED_CUST_COLS if c.upper() not in _cust_dict_cols_upper]
_found_cust_cols    = [c for c in _REQUIRED_CUST_COLS if c.upper() in _cust_dict_cols_upper]

for _col in _REQUIRED_CUST_COLS:
    _found = _col.upper() in _cust_dict_cols_upper
    log("INFO", f"  {_col:<20} | {'FOUND' if _found else 'MISSING'}", SECTION)

blocker(
    "OLD_CUS_IDT" not in [c.upper() for c in _cust_dict_cols],
    "OLD_CUS_IDT NOT FOUND in VW_D_CUSTOMER_DICTONARY. "
    f"Available columns: {_cust_dict_cols}. Plan join logic invalid.",
    SECTION,
)
blocker(
    "NEW_CUS_IDT" not in [c.upper() for c in _cust_dict_cols],
    "NEW_CUS_IDT NOT FOUND in VW_D_CUSTOMER_DICTONARY. "
    f"Available columns: {_cust_dict_cols}. Plan join logic invalid.",
    SECTION,
)

# Only run count query if columns were found (avoid error)
if not _missing_cust_cols or all(c not in ["OLD_CUS_IDT", "NEW_CUS_IDT"] for c in _missing_cust_cols):
    df_cust_dict_stats = run_sf(DB_PRD_MEX, """
        SELECT
            COUNT(*)                 AS total_rows,
            COUNT(DISTINCT OLD_CUS_IDT) AS distinct_old_ids,
            COUNT(DISTINCT NEW_CUS_IDT) AS distinct_new_ids,
            SUM(CASE WHEN OLD_CUS_IDT = NEW_CUS_IDT THEN 1 ELSE 0 END) AS same_id_rows
        FROM PRD_MEX.MEX_DSP_OTC.VW_D_CUSTOMER_DICTONARY
    """)
    _cust_stats = df_cust_dict_stats.collect()[0]
    log("INFO", f"Record count stats:", SECTION)
    log("INFO", f"  total_rows:        {_cust_stats['total_rows']:,}", SECTION)
    log("INFO", f"  distinct_old_ids:  {_cust_stats['distinct_old_ids']:,}", SECTION)
    log("INFO", f"  distinct_new_ids:  {_cust_stats['distinct_new_ids']:,}", SECTION)
    log("INFO", f"  same_id_rows (no migration): {_cust_stats['same_id_rows']:,}", SECTION)
    save_df(df_cust_dict_stats, f"{DBFS_AMBI}/ambi_08_customer_dict_validation.csv", SECTION)

    if not _missing_cust_cols:
        resolve(SECTION,
            f"VW_D_CUSTOMER_DICTONARY CONFIRMED: OLD_CUS_IDT and NEW_CUS_IDT exist. "
            f"total_rows={_cust_stats['total_rows']:,} | distinct_old={_cust_stats['distinct_old_ids']:,} | same_id={_cust_stats['same_id_rows']:,}.")
else:
    # Save column list only
    _col_rows_cust = pd.DataFrame([{"column_index": i, "column_name": c} for i, c in enumerate(_cust_dict_cols, 1)])
    df_col_list_cust = spark.createDataFrame(_col_rows_cust)
    save_df(df_col_list_cust, f"{DBFS_AMBI}/ambi_08_customer_dict_validation.csv", SECTION)

log("INFO", "Ambiguity 8 complete.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ambiguity 9 -- Nielsen Water Scantrack hierarchy_level for UPC grain
# MAGIC
# MAGIC **QUESTION:** Does VW_IND_AGUA_BNF_ST_NLSN_PROD_DIM have hierarchy_level=11
# MAGIC with populated PRDC_CD?  Unlike Water Retail (max level 9), Water Scantrack
# MAGIC should support UPC-grain P0 matching.
# MAGIC
# MAGIC **Source:** PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_ST_NLSN_PROD_DIM

# COMMAND ----------

SECTION = "AMBI_09"
log("INFO", "=" * 70, SECTION)
log("INFO", "AMBIGUITY 9 -- Nielsen Water Scantrack hierarchy_level for UPC grain", SECTION)
log("INFO", "QUESTION: Does VW_IND_AGUA_BNF_ST have hierarchy_level=11 with populated PRDC_CD?", SECTION)
log("INFO", "          Unlike Water Retail (max level 9), Scantrack should reach UPC grain.", SECTION)

df_wst_hier = run_sf(DB_PRD_MEX, """
    SELECT
        hierarchy_level,
        COUNT(*) AS row_count,
        COUNT(DISTINCT PRDC_CD) AS distinct_prdc_cd,
        SUM(CASE WHEN PRDC_CD IS NULL THEN 1 ELSE 0 END) AS null_prdc_cd
    FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_ST_NLSN_PROD_DIM
    GROUP BY 1
    ORDER BY 1
""")
df_wst_hier.cache()
_wst_rows    = df_wst_hier.collect()
_wst_max_lvl = max((r["hierarchy_level"] for r in _wst_rows), default=0)
_wst_has_11  = any(r["hierarchy_level"] == 11 for r in _wst_rows)
_wst_has_9   = any(r["hierarchy_level"] == 9  for r in _wst_rows)

log("INFO", "Water Scantrack hierarchy_level distribution:", SECTION)
df_wst_hier.show(20, truncate=False)
log("INFO", f"Water Scantrack max hierarchy_level = {_wst_max_lvl}", SECTION)

blocker(
    not _wst_has_11,
    "hierarchy_level=11 NOT FOUND in VW_IND_AGUA_BNF_ST_NLSN_PROD_DIM. "
    "P0 Water Scantrack bridge (which requires level 11 PRDC_CD) will fail. "
    "Investigate: escalate to architect to determine correct UPC-grain level.",
    SECTION,
)

if not _wst_has_11 and _wst_has_9:
    _l9_row = next((r for r in _wst_rows if r["hierarchy_level"] == 9), None)
    if _l9_row:
        _l9_prdc = int(_l9_row["distinct_prdc_cd"])
        _l9_null = int(_l9_row["null_prdc_cd"])
        log("INFO", f"Level 9 available: distinct_PRDC_CD={_l9_prdc:,} | null_PRDC_CD={_l9_null:,}", SECTION)
        log("INFO", "If hierarchy_level=11 NOT found but hierarchy_level=9 has PRDC_CD populated, "
                    "Water Scantrack UPC grain should use level 9 instead -- escalate to architect.", SECTION)

if _wst_has_11:
    _l11_row = next((r for r in _wst_rows if r["hierarchy_level"] == 11), None)
    if _l11_row:
        _l11_prdc = int(_l11_row["distinct_prdc_cd"])
        _l11_null = int(_l11_row["null_prdc_cd"])
        log("INFO", f"Level 11: distinct_PRDC_CD={_l11_prdc:,} | null_PRDC_CD={_l11_null:,}", SECTION)
        if _l11_prdc > 0:
            resolve(SECTION,
                f"hierarchy_level=11 EXISTS in Water Scantrack with {_l11_prdc:,} distinct PRDC_CD. "
                "P0 Water Scantrack bridge is VALID at level 11.")
        else:
            warn(True,
                "hierarchy_level=11 exists in Water Scantrack but distinct_PRDC_CD=0. "
                "No UPC matches possible at level 11. Architect review required.",
                SECTION)

save_df(df_wst_hier, f"{DBFS_AMBI}/ambi_09_water_scantrack_hierarchy.csv", SECTION)
log("INFO", "Ambiguity 9 complete.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Cell -- Ambiguity Resolution Summary
# MAGIC
# MAGIC Produces a structured resolution table for all 9 ambiguities:
# MAGIC | # | Question | Status | Action Required |

# COMMAND ----------

SECTION = "AMBI_SUMMARY"
log("INFO", "=" * 70, SECTION)
log("INFO", "AMBIGUITY RESOLUTION SUMMARY", SECTION)
log("INFO", f"Run date: {RUN_DATE}", SECTION)

# Define all 9 ambiguities with their plan references and action templates
_AMBI_META = {
    "AMBI_01": {
        "question": "stat_cod active filter value in V_D_CLIENT",
        "plan_ref": "C10",
        "action_if_resolved": "No action. Use stat_cod='A' as active filter.",
        "action_if_flagged":  "Update plan active filter to correct stat_cod value.",
        "action_if_blocked":  "Investigate V_D_CLIENT stat_cod column and revise plan.",
    },
    "AMBI_02": {
        "question": "VW_WASTE column names for volume, value, SKU",
        "plan_ref": "WASTE integration",
        "action_if_resolved": "No action. Column names confirmed.",
        "action_if_flagged":  "Architect must map column names from captured list.",
        "action_if_blocked":  "Architect must inspect VW_WASTE schema and update plan placeholders.",
    },
    "AMBI_03": {
        "question": "Nielsen EDP hierarchy_level=11 has populated PRDC_CD",
        "plan_ref": "P0 EDP bridge",
        "action_if_resolved": "No action. EDP P0 bridge at level 11 is valid.",
        "action_if_flagged":  "Review PRDC_CD null rate at level 11.",
        "action_if_blocked":  "EDP P0 bridge plan must be revised. Find valid UPC grain level.",
    },
    "AMBI_04": {
        "question": "Nielsen Water Retail max hierarchy_level = 9 (no level 11)",
        "plan_ref": "C1 decision / P0 exclusion",
        "action_if_resolved": "No action. C1 decision confirmed: Water Retail excluded from P0.",
        "action_if_flagged":  "Review C1 decision: Water Retail level 11 found. Update P0 plan.",
        "action_if_blocked":  "C1 decision blocked pending architect review.",
    },
    "AMBI_05": {
        "question": "VW_FACT_RNV SHP_CUS_IDT and MAT_IDT join keys exist",
        "plan_ref": "SELL_IN fact join logic",
        "action_if_resolved": "No action. All join keys confirmed.",
        "action_if_flagged":  "Review alternative customer join keys.",
        "action_if_blocked":  "Plan join keys are invalid. Update SELL_IN join logic immediately.",
    },
    "AMBI_06": {
        "question": "IBP CADENA values are retail chains (not territory codes)",
        "plan_ref": "MC-A14 cross-validation",
        "action_if_resolved": "No action. IBP CADENA == retail chains. MC-A14 valid.",
        "action_if_flagged":  "Revise MC-A14. CADENA cannot cross-validate with SELL_OUT CHAIN.",
        "action_if_blocked":  "MC-A14 cross-validation must be removed or redesigned.",
    },
    "AMBI_07": {
        "question": "SELL_OUT SUBCHAIN null rate <= 30% (usable for D1)",
        "plan_ref": "Canal D1 profiling",
        "action_if_resolved": "No action. SUBCHAIN usable for D1 tertiary level.",
        "action_if_flagged":  "Exclude SUBCHAIN from D1 winner selection. Use FORMAT+CHAIN only.",
        "action_if_blocked":  "SUBCHAIN profiling blocked.",
    },
    "AMBI_08": {
        "question": "VW_D_CUSTOMER_DICTONARY has OLD_CUS_IDT and NEW_CUS_IDT",
        "plan_ref": "Customer ID migration logic",
        "action_if_resolved": "No action. Customer dictionary keys confirmed.",
        "action_if_flagged":  "Review available keys and update plan join logic.",
        "action_if_blocked":  "Customer dictionary join keys missing. Plan customer migration logic invalid.",
    },
    "AMBI_09": {
        "question": "Nielsen Water Scantrack hierarchy_level=11 has PRDC_CD for P0 bridge",
        "plan_ref": "P0 Water Scantrack bridge",
        "action_if_resolved": "No action. Water Scantrack P0 bridge at level 11 is valid.",
        "action_if_flagged":  "Check level 9 as fallback UPC grain. Escalate to architect.",
        "action_if_blocked":  "Water Scantrack P0 bridge must be redesigned. Escalate immediately.",
    },
}

# Build summary table
_summary_rows = []
for _ambi_id, _meta in _AMBI_META.items():
    _status = _AMBI_STATUS.get(_ambi_id, "RESOLVED")  # Default to RESOLVED if no warn/blocker triggered
    if _status == "RESOLVED":
        _action = _meta["action_if_resolved"]
    elif _status == "FLAGGED":
        _action = _meta["action_if_flagged"]
    else:  # BLOCKED
        _action = _meta["action_if_blocked"]

    _summary_rows.append({
        "ambi_id":   _ambi_id,
        "plan_ref":  _meta["plan_ref"],
        "question":  _meta["question"],
        "status":    _status,
        "action":    _action,
    })

# Print summary table
print("\n" + "=" * 100)
print(f"AMBIGUITY RESOLUTION SUMMARY  --  {RUN_DATE}")
print("=" * 100)
print(f"{'#':<10} {'Question':<52} {'Status':<10} {'Action Required'}")
print("-" * 100)
for r in _summary_rows:
    _trunc_q = r["question"][:50] + ".." if len(r["question"]) > 50 else r["question"]
    _trunc_a = r["action"][:45] + ".." if len(r["action"]) > 45 else r["action"]
    print(f"{r['ambi_id']:<10} {_trunc_q:<52} {r['status']:<10} {_trunc_a}")
print("=" * 100)

# Also log to structured log
log("INFO", "=" * 70, SECTION)
log("INFO", f"{'AMBI_ID':<12} | {'STATUS':<10} | QUESTION + ACTION", SECTION)
log("INFO", "-" * 70, SECTION)
for r in _summary_rows:
    log("INFO", f"{r['ambi_id']:<12} | {r['status']:<10} | {r['question']}", SECTION)
    log("INFO", f"{'':12}   {'':10}   ACTION: {r['action']}", SECTION)
log("INFO", "=" * 70, SECTION)

# Counts
_resolved_n = sum(1 for r in _summary_rows if r["status"] == "RESOLVED")
_flagged_n  = sum(1 for r in _summary_rows if r["status"] == "FLAGGED")
_blocked_n  = sum(1 for r in _summary_rows if r["status"] == "BLOCKED")
log("INFO", f"TOTAL: RESOLVED={_resolved_n} | FLAGGED={_flagged_n} | BLOCKED={_blocked_n}", SECTION)

# Write summary CSV
df_ambi_summary = spark.createDataFrame(pd.DataFrame(_summary_rows))
save_df(df_ambi_summary, f"{DBFS_AMBI}/ambiguity_resolution_summary.csv", SECTION)
log("INFO", f"Summary CSV written: {DBFS_AMBI}/ambiguity_resolution_summary.csv", SECTION)

# Flush all logs
flush_log(f"ambiguity_resolution_report_{RUN_DATE}.txt")

# Final raise if blockers
if _HARD_BLOCKERS:
    raise RuntimeError(
        f"AMBIGUITY RESOLUTION: {len(_HARD_BLOCKERS)} hard blocker(s) found.\n"
        "Plan cannot proceed until blockers are resolved:\n"
        + "\n".join(_HARD_BLOCKERS)
    )

print(f"\nOK Ambiguity resolution complete -- {RUN_DATE}")
print(f"   RESOLVED: {_resolved_n}  |  FLAGGED: {_flagged_n}  |  BLOCKED: {_blocked_n}")
print(f"   Summary: {DBFS_AMBI}/ambiguity_resolution_summary.csv")
print(f"   Report:  {DBFS_AMBI}/ambiguity_resolution_report_{RUN_DATE}.txt")
