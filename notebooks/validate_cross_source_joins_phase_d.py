# Databricks notebook source
# MAGIC %md
# MAGIC # V13 — Cross-Source Join Validation (Phase D — Stage 1)
# MAGIC
# MAGIC **Grain:** `MARCA_STD + MONTH` (brand-month, no CADENA/CANAL)
# MAGIC
# MAGIC **Checks:**
# MAGIC - V13A: SELL_OUT × MKT_ON      (warn if < 70% brand overlap)
# MAGIC - V13B: SELL_OUT × MKT_OFF     (warn if < 70%, Danone only)
# MAGIC - V13C: SELL_OUT × WASTE       (warn if < 80%)
# MAGIC - V13D: SELL_IN × IBP          (warn if < 80%)
# MAGIC - V13E: SELL_OUT × EDP Nielsen (warn if < 60%)
# MAGIC
# MAGIC **Hard blockers stop execution immediately.**
# MAGIC **Warnings are logged but never stop execution.**
# MAGIC
# MAGIC **Log output:** `notebooks/validation_results_phase_d.txt`
# MAGIC
# MAGIC Run in Databricks. Commit log after successful run.

# COMMAND ----------

import os
import sys
import io
import uuid
import importlib.util
from datetime import datetime

# ═══════════════════════════════════════════════════════════════════════════════
# Credential resolution — identical pattern to V12 / validate_credentials.py
# Priority: (1) configs/snowflake_creds.py  (2) Databricks KV  (3) env var
# Cross-DB note: PRD_MEX + PRD_MDP cannot share one Snowflake session (ISSUE-003).
# ═══════════════════════════════════════════════════════════════════════════════

SF_URL        = "danonenam.east-us-2.azure.snowflakecomputing.com"
KV_SCOPE_MEX  = "DAN-AM-P-KVT800-R-MEX-DB"
KV_SCOPE_MDP  = "DAN-AM-P-KVT800-R-MDP-DB"

_creds = None

def _find_creds_file():
    """
    Try multiple candidate locations for snowflake_creds.py.
    Databricks notebooks have no __file__, so we probe well-known paths.
    Resolution order:
      1. Same directory as this notebook (./configs/snowflake_creds.py)
      2. CWD/../configs/snowflake_creds.py
      3. /Workspace/…/configs/snowflake_creds.py  (Databricks workspace root)
      4. DBFS: /dbfs/…/configs/snowflake_creds.py
    """
    candidates = []
    # 1. Relative to __file__ when available (local / pytest)
    try:
        candidates.append(
            os.path.normpath(
                os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "configs", "snowflake_creds.py")
            )
        )
    except Exception:
        pass
    # 2. CWD-based (Databricks sets CWD to the notebook's directory)
    try:
        cwd = os.getcwd()
        candidates.append(os.path.join(cwd, "configs", "snowflake_creds.py"))
        candidates.append(os.path.normpath(os.path.join(cwd, "..", "configs", "snowflake_creds.py")))
        # If we are already inside /notebooks, go up one level
        if os.path.basename(cwd).lower() == "notebooks":
            candidates.append(os.path.join(os.path.dirname(cwd), "configs", "snowflake_creds.py"))
    except Exception:
        pass
    # 3. Databricks Repos path (/Workspace/Repos/<user>/<repo>/configs/...)
    try:
        import subprocess
        repo_root = subprocess.check_output(
            ["git", "rev-parse", "--show-toplevel"], stderr=subprocess.DEVNULL
        ).decode().strip()
        candidates.append(os.path.join(repo_root, "configs", "snowflake_creds.py"))
    except Exception:
        pass
    return candidates

for _creds_path in _find_creds_file():
    try:
        if os.path.exists(_creds_path):
            _spec = importlib.util.spec_from_file_location("snowflake_creds", _creds_path)
            _m    = importlib.util.module_from_spec(_spec)
            _spec.loader.exec_module(_m)
            _creds = _m
            print(f"  [credentials] Loaded from {_creds_path}")
            break
    except Exception as _e:
        print(f"  [credentials] Could not load {_creds_path}: {_e}")

if _creds is None:
    print("  [credentials] snowflake_creds.py not found via filesystem — will try Databricks KV next")


def _secret(local_val, scope, key, env_var=None):
    """Resolve credential: (1) local creds file → (2) Databricks KV → (3) env var."""
    if local_val is not None:
        return local_val
    try:
        return dbutils.secrets.get(scope=scope, key=key)      # noqa: F821
    except Exception:
        pass
    val = os.getenv(env_var) if env_var else None
    if val:
        return val
    raise RuntimeError(
        f"Cannot resolve '{key}'. "
        f"Ensure configs/snowflake_creds.py exists on the Databricks workspace "
        f"(copy from configs/snowflake_creds.example.py and fill in values), "
        f"OR that Databricks KV scope '{scope}' contains key '{key}'."
    )


# ─── Connection profiles ────────────────────────────────────────────────────
# PRD_MEX — role: PRD_MEX_READER  (Nielsen, SELL_IN)
PRD_MEX_PROFILE = {
    "sfURL":       SF_URL,
    "sfUser":      _secret(getattr(_creds, "SF_MEX_USER",     None), KV_SCOPE_MEX, "snowflake-mex-user",     "SF_MEX_USER"),
    "sfPassword":  _secret(getattr(_creds, "SF_MEX_PASSWORD", None), KV_SCOPE_MEX, "snowflake-mex-password", "SF_MEX_PASSWORD"),
    "sfWarehouse": getattr(_creds, "SF_MEX_WH",   "PRD_MEX_ANL_WH"),
    "sfRole":      getattr(_creds, "SF_MEX_ROLE",  "PRD_MEX_READER"),
}

# PRD_MDP — role: PRD_MDP  (NOT "PRD_MDP_READER" — see ISSUE-002)
# Target schema for mart: PRD_MPD.MDP_STG
PRD_MDP_PROFILE = {
    "sfURL":       SF_URL,
    "sfUser":      _secret(getattr(_creds, "SF_MDP_USER",     None), KV_SCOPE_MDP, "snowflake-user",     "SF_MDP_USER"),
    "sfPassword":  _secret(getattr(_creds, "SF_MDP_PASSWORD", None), KV_SCOPE_MDP, "snowflake-password", "SF_MDP_PASSWORD"),
    "sfWarehouse": getattr(_creds, "SF_MDP_WH",   "PRD_MDP_ANL_WH"),
    "sfRole":      getattr(_creds, "SF_MDP_ROLE",  "PRD_MDP"),
}

# ─── Run metadata ────────────────────────────────────────────────────────────
RUN_ID = str(uuid.uuid4())
RUN_TS = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ─── Logging ─────────────────────────────────────────────────────────────────
LOG_LINES  = []
BLOCKERS   = []   # Accumulates hard-blocker messages
WARNINGS   = []   # Accumulates warning messages


def log(msg=""):
    ts   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    LOG_LINES.append(line)


def log_df(df, label, n=200):
    log(f"  {label}:")
    old_stdout = sys.stdout
    buf        = io.StringIO()
    try:
        sys.stdout = buf
        df.show(n, truncate=False)
    finally:
        sys.stdout = old_stdout
    out = buf.getvalue()
    print(out)
    for l in out.rstrip().split("\n"):
        LOG_LINES.append(l)


def blocker(msg):
    """Register a hard blocker and abort after the current check section."""
    full = f"[BLOCKER] {msg}"
    log(full)
    BLOCKERS.append(full)


def warn(msg):
    full = f"[WARNING] {msg}"
    log(full)
    WARNINGS.append(full)


def abort_if_blockers():
    """Raise RuntimeError if any hard blockers have accumulated."""
    if BLOCKERS:
        summary = "\n".join(BLOCKERS)
        raise RuntimeError(
            f"\n{'='*70}\n"
            f"EXECUTION ABORTED — {len(BLOCKERS)} hard blocker(s) detected:\n"
            f"{summary}\n"
            f"{'='*70}\n"
            f"Fix the above issues before proceeding to Stage 2.\n"
        )


def run_sf_query(database, query, label="query"):
    profile = PRD_MDP_PROFILE if database == "PRD_MDP" else PRD_MEX_PROFILE
    opts    = {**profile, "sfDatabase": database}
    log(f"  Running: {label}  [db={database}]")
    df = (
        spark.read                                                # noqa: F821
        .format("net.snowflake.spark.snowflake")
        .options(**opts)
        .option("sfDatabase", database)
        .option("query", query)
        .load()
    )
    rc = df.count()
    log(f"  → {rc} rows returned")
    return df, rc


# ─── Log file path resolution (mirrors V12 pattern) ──────────────────────────
def get_log_path():
    candidates = []
    try:
        cwd    = os.getcwd()
        nb_dir = os.path.join(cwd, "notebooks")
        if os.path.isdir(nb_dir):
            candidates.append(os.path.join(nb_dir, "validation_results_phase_d.txt"))
        if os.path.basename(cwd).lower() == "notebooks":
            candidates.insert(0, os.path.join(cwd, "validation_results_phase_d.txt"))
        candidates.append(os.path.join(cwd, "validation_results_phase_d.txt"))
    except Exception:
        pass
    candidates.append("/tmp/validation_results_phase_d.txt")
    return candidates


# COMMAND ----------

# MAGIC %md
# MAGIC ## Header

log("=" * 70)
log("V13 — Phase D Stage 1: Cross-Source Join Validation")
log("=" * 70)
log(f"Run ID        : {RUN_ID}")
log(f"Run timestamp : {RUN_TS}")
log(f"Mart target   : PRD_MPD.MDP_STG.MART_MARKET_GROWTH_INTELLIGENCE_MONTHLY")
log(f"Grain         : MARCA_STD + MONTH (brand-month, no CADENA/CANAL)")
log(f"Checks        : V13A SELL_OUT×MKT_ON | V13B SELL_OUT×MKT_OFF | V13C SELL_OUT×WASTE")
log(f"                V13D SELL_IN×IBP     | V13E SELL_OUT×EDP Nielsen")
log("")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Danone brand list — loaded from brand_crosswalk.yaml at runtime

# COMMAND ----------

import yaml, pathlib

_crosswalk_path = pathlib.Path(__file__).resolve().parent.parent / "configs" / "brand_crosswalk.yaml"
with open(_crosswalk_path, "r", encoding="utf-8") as _f:
    _crosswalk = yaml.safe_load(_f)

DANONE_BRANDS = list(_crosswalk.get("danone_brands", {}).keys())

log(f"Danone brands loaded from brand_crosswalk.yaml: {len(DANONE_BRANDS)} brands")
log(f"  → {sorted(DANONE_BRANDS)}")
log("")

# Build SQL IN-list string for Snowflake
_danone_sql_list = ", ".join(f"'{b}'" for b in DANONE_BRANDS)
DANONE_IN_CLAUSE = f"({_danone_sql_list})"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Access Pre-check — PRD_MPD.MDP_STG
# MAGIC Hard blocker: if we cannot read the target schema, Stage 2 write will fail.

# COMMAND ----------

log("=" * 70)
log("PRE-CHECK — PRD_MPD.MDP_STG schema access")
log("=" * 70)

schema_check_sql = "SHOW TABLES IN SCHEMA PRD_MDP.MDP_STG"
try:
    df_schema, _ = run_sf_query("PRD_MDP", schema_check_sql, "Schema access: SHOW TABLES IN PRD_MDP.MDP_STG")
    log("  ✅ PRD_MPD.MDP_STG schema is accessible under PRD_MDP role")
except Exception as e:
    blocker(
        f"PRD_MPD.MDP_STG schema not accessible: {e}. "
        f"DBA must grant CREATE TABLE + INSERT on PRD_MPD.MDP_STG to role PRD_MDP."
    )

abort_if_blockers()

# COMMAND ----------

# MAGIC %md
# MAGIC ## V13-PRE: SELL_OUT MARCA_STD NULL check (zero-tolerance hard blocker)

# COMMAND ----------

log("=" * 70)
log("V13-PRE — SELL_OUT MARCA_STD zero-tolerance NULL check")
log("=" * 70)
log("Rule: SELL_OUT MARCA_STD NULL rate must be exactly 0%. Any NULL = BLOCKER.")
log("")

sell_out_null_sql = """
SELECT
    COUNT(*)                                                           AS TOTAL_ROWS,
    SUM(CASE WHEN MARCA_STD IS NULL THEN 1 ELSE 0 END)                AS NULL_MARCA_STD,
    SUM(CASE WHEN MARCA_STD IS NULL THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(*), 0)                                          AS NULL_PCT,
    COUNT(DISTINCT MARCA_STD)                                          AS DISTINCT_BRANDS,
    COUNT(DISTINCT DATE_TRUNC('MONTH', FECHA))                         AS DISTINCT_MONTHS,
    MIN(DATE_TRUNC('MONTH', FECHA))                                    AS MIN_MONTH,
    MAX(DATE_TRUNC('MONTH', FECHA))                                    AS MAX_MONTH
FROM (
    SELECT
        per."DAY"   AS FECHA,
        prod.BRAND  AS MARCA,
        CASE
            WHEN TRIM(UPPER(prod.BRAND)) IN ('ACTIVIA') THEN 'ACTIVIA'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('AGUAS FRESCAS','BFT AGUAS FRESCAS','BONAFONT_AGUASFRESCAS','BONAFONT AGUA FRESCAS') THEN 'AGUAS FRESCAS'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('BADOIT') THEN 'BADOIT'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('BENEGASTRO','BENEG') THEN 'BENEGASTRO'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('BONAFONT','BONAFONT_NATURAL','AZUL BONAFONT','BONAFONT NATURAL','WATER BONAFONT','BONAFONT 11 LTS','Bft 11 Lts','Bft 20 Lts','Salmon Bonafont') THEN 'BONAFONT'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('BONAFONT JUGO') THEN 'BONAFONT JUGO'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('BONAFONT KIDS','BONAFONT_KIDS','BONAFONT KIDS NATURAL','Kids','KIDS') THEN 'BONAFONT KIDS'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('BONAFONT MINERAL','MINERALIZADA','Mineralizada') THEN 'BONAFONT MINERAL'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('BONAFONT SER','SER PURA') THEN 'BONAFONT SER'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('TE BONAFONT','TÉ BONAFONT','BONAFONT TE','BONAFONT_TE','BONAFONT TÉ') THEN 'BONAFONT TE'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('DANETTE') THEN 'DANETTE'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('DANISSIMO') THEN 'DANISSIMO'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('DANMIX','DAN MIX','DANMMIX') THEN 'DANMIX'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('DANONE','DANONE YOGHURT','DANONE CREME','DANONE FREE','DANONE GRIEGO','DANONE FS','DAIRY','Danone','DAIRY DANONE MEXICO','Dairy Danone France','DANAO') THEN 'DANONE'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('DANONINO','DANONINOLIQUIDO') THEN 'DANONINO'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('DANUP','DAN UP','DAN''UP') THEN 'DANUP'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('DANY','DANY DANETTE') THEN 'DANY'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('DELIGHT') THEN 'DELIGHT'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('EVIAN','FERRARELLE') THEN 'EVIAN'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('HERSHEYS','HERSHEY''S','DANONE HERSHEYS') THEN 'HERSHEYS'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('INFINIT WATER','INFINIT','INFINITY','WATER INFINIT') THEN 'INFINIT'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('JUIZZY','Juizzy','FRUIX') THEN 'JUIZZY'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('LEVITE','BONAFONT_LEVITE','BONAFONT LEVITE','Levite','LEVITE CLASICA','LEVITE INFUSIONES','LEVITE CERO','LEVITE BALANCE') THEN 'LEVITE'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('LICUAMIX') THEN 'LICUAMIX'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('OCEAN SPRAY','OCEAN') THEN 'OCEAN SPRAY'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('OIKOS','OIKOS UHT') THEN 'OIKOS'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('PUREZA AGA','AGA','AGA 20 LTS','AQUAPURA','AGUA NATURAL','Aga','Aga 20 Lts','Agua Natural','Ultrapura','ULTRAPURA','Botella Aga','AGA PUREZA') THEN 'PUREZA AGA'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('SILK','SILK ORIG 946ML','SILKCHOCO190ML','Silk') THEN 'SILK'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('SO DELICIOUS') THEN 'SO DELICIOUS'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('STOK COLD BREW','STOK') THEN 'STOK'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('VITALINEA') THEN 'VITALINEA'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('YOPRO','YO PRO') THEN 'YOPRO'
            ELSE TRIM(UPPER(prod.BRAND))
        END AS MARCA_STD
    FROM PRD_MDP.MDP_DSP.VW_FACT_SELL_OUT  f
    INNER JOIN PRD_MDP.MDP_DWH.V_D_PERIOD  per  ON f.PER_ID = per.PER_ID
    INNER JOIN PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM prod
        ON TO_VARCHAR(f.UPC) = TO_VARCHAR(prod.INT_ID)
        AND f.CBU_ID = prod.CBU_ID
) t
"""

try:
    df_so_null, _ = run_sf_query("PRD_MDP", sell_out_null_sql, "V13-PRE: SELL_OUT MARCA_STD null check")
    log_df(df_so_null, "SELL_OUT MARCA_STD summary", n=5)
    row        = df_so_null.collect()[0]
    null_cnt   = int(row["NULL_MARCA_STD"] or 0)
    null_pct   = float(row["NULL_PCT"]     or 0.0)
    total_rows = int(row["TOTAL_ROWS"]     or 0)
    log(f"  SELL_OUT rows: {total_rows:,}  |  NULL MARCA_STD: {null_cnt:,}  ({null_pct:.4f}%)")
    if null_cnt > 0:
        blocker(
            f"SELL_OUT MARCA_STD NULL count = {null_cnt} ({null_pct:.4f}%). "
            f"Zero tolerance — all SELL_OUT rows must resolve MARCA_STD. Investigate unmapped BRAND values."
        )
    else:
        log("  ✅ V13-PRE PASS — SELL_OUT MARCA_STD: 0 NULLs")

    # DATE_TRUNC test
    if total_rows > 0 and row["MIN_MONTH"] is None:
        blocker("FECHA DATE_TRUNC returned NULL for all SELL_OUT rows — date column may be uncastable.")
    else:
        log(f"  ✅ DATE_TRUNC check — MIN_MONTH={row['MIN_MONTH']}  MAX_MONTH={row['MAX_MONTH']}")

    # Capture pre-join SELL_OUT totals for reconciliation in Stage 2
    SO_DISTINCT_BRANDS = int(row["DISTINCT_BRANDS"] or 0)
    SO_DISTINCT_MONTHS = int(row["DISTINCT_MONTHS"] or 0)
    SO_MAX_MONTH       = row["MAX_MONTH"]
    log(f"  Distinct brands: {SO_DISTINCT_BRANDS}  |  Distinct months: {SO_DISTINCT_MONTHS}")

except Exception as e:
    blocker(f"V13-PRE query failed: {e}")
    SO_DISTINCT_BRANDS = None
    SO_DISTINCT_MONTHS = None
    SO_MAX_MONTH       = None

abort_if_blockers()

# COMMAND ----------

# MAGIC %md
# MAGIC ## V13A — SELL_OUT × MKT_ON brand match at MARCA_STD + MONTH grain
# MAGIC Warning threshold: < 70% of SELL_OUT Danone brands matched in MKT_ON

# COMMAND ----------

log("=" * 70)
log("V13A — SELL_OUT × MKT_ON: brand overlap at MARCA_STD + MONTH grain")
log("Threshold: WARN if < 70% of SELL_OUT Danone brands matched in MKT_ON")
log("=" * 70)

v13a_sql = f"""
WITH sell_out_brands AS (
    SELECT DISTINCT
        CASE
            WHEN TRIM(UPPER(prod.BRAND)) IN ('ACTIVIA') THEN 'ACTIVIA'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('AGUAS FRESCAS','BFT AGUAS FRESCAS','BONAFONT_AGUASFRESCAS','BONAFONT AGUA FRESCAS') THEN 'AGUAS FRESCAS'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('BADOIT') THEN 'BADOIT'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('BENEGASTRO','BENEG') THEN 'BENEGASTRO'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('BONAFONT','BONAFONT_NATURAL','AZUL BONAFONT','BONAFONT NATURAL','WATER BONAFONT','BONAFONT 11 LTS') THEN 'BONAFONT'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('BONAFONT JUGO') THEN 'BONAFONT JUGO'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('BONAFONT KIDS','BONAFONT_KIDS','BONAFONT KIDS NATURAL','Kids','KIDS') THEN 'BONAFONT KIDS'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('BONAFONT MINERAL','MINERALIZADA','Mineralizada') THEN 'BONAFONT MINERAL'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('BONAFONT SER','SER PURA') THEN 'BONAFONT SER'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('TE BONAFONT','TÉ BONAFONT','BONAFONT TE','BONAFONT_TE','BONAFONT TÉ') THEN 'BONAFONT TE'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('DANETTE') THEN 'DANETTE'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('DANISSIMO') THEN 'DANISSIMO'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('DANMIX','DAN MIX','DANMMIX') THEN 'DANMIX'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('DANONE','DANONE YOGHURT','DANONE CREME','DANONE FREE','DANONE GRIEGO','DANONE FS','DAIRY','Danone','DAIRY DANONE MEXICO','DANAO') THEN 'DANONE'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('DANONINO','DANONINOLIQUIDO') THEN 'DANONINO'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('DANUP','DAN UP','DAN''UP') THEN 'DANUP'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('DANY','DANY DANETTE') THEN 'DANY'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('DELIGHT') THEN 'DELIGHT'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('EVIAN','FERRARELLE') THEN 'EVIAN'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('HERSHEYS','HERSHEY''S','DANONE HERSHEYS') THEN 'HERSHEYS'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('INFINIT WATER','INFINIT','INFINITY','WATER INFINIT') THEN 'INFINIT'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('JUIZZY','Juizzy','FRUIX') THEN 'JUIZZY'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('LEVITE','BONAFONT_LEVITE','BONAFONT LEVITE','Levite','LEVITE CLASICA','LEVITE INFUSIONES','LEVITE CERO','LEVITE BALANCE') THEN 'LEVITE'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('LICUAMIX') THEN 'LICUAMIX'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('OCEAN SPRAY','OCEAN') THEN 'OCEAN SPRAY'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('OIKOS','OIKOS UHT') THEN 'OIKOS'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('PUREZA AGA','AGA','AGA 20 LTS','AQUAPURA','AGUA NATURAL','Aga','Aga 20 Lts','Agua Natural','Ultrapura','ULTRAPURA','Botella Aga','AGA PUREZA') THEN 'PUREZA AGA'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('SILK','SILK ORIG 946ML','SILKCHOCO190ML','Silk') THEN 'SILK'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('SO DELICIOUS') THEN 'SO DELICIOUS'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('STOK COLD BREW','STOK') THEN 'STOK'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('VITALINEA') THEN 'VITALINEA'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('YOPRO','YO PRO') THEN 'YOPRO'
            ELSE TRIM(UPPER(prod.BRAND))
        END AS MARCA_STD
    FROM PRD_MDP.MDP_DSP.VW_FACT_SELL_OUT f
    INNER JOIN PRD_MDP.MDP_DWH.V_D_PERIOD per  ON f.PER_ID = per.PER_ID
    INNER JOIN PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM prod
        ON TO_VARCHAR(f.UPC) = TO_VARCHAR(prod.INT_ID)
        AND f.CBU_ID = prod.CBU_ID
    WHERE TRIM(UPPER(prod.BRAND)) IN {DANONE_IN_CLAUSE.replace("(", "").replace(")", "")}
       OR CASE
            WHEN TRIM(UPPER(prod.BRAND)) IN ('ACTIVIA') THEN 'ACTIVIA'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('AGUAS FRESCAS','BFT AGUAS FRESCAS','BONAFONT_AGUASFRESCAS','BONAFONT AGUA FRESCAS') THEN 'AGUAS FRESCAS'
            ELSE TRIM(UPPER(prod.BRAND))
          END IN {DANONE_IN_CLAUSE}
),
mkt_on_brands AS (
    SELECT DISTINCT
        CASE
            WHEN TRIM(UPPER(MARCA)) IN ('ACTIVIA') THEN 'ACTIVIA'
            WHEN TRIM(UPPER(MARCA)) IN ('AGUAS FRESCAS','BFT AGUAS FRESCAS','BONAFONT_AGUASFRESCAS','BONAFONT AGUA FRESCAS') THEN 'AGUAS FRESCAS'
            WHEN TRIM(UPPER(MARCA)) IN ('BENEGASTRO','BENEG') THEN 'BENEGASTRO'
            WHEN TRIM(UPPER(MARCA)) IN ('BONAFONT','BONAFONT_NATURAL','AZUL BONAFONT') THEN 'BONAFONT'
            WHEN TRIM(UPPER(MARCA)) IN ('BONAFONT KIDS','BONAFONT_KIDS','BONAFONT KIDS NATURAL') THEN 'BONAFONT KIDS'
            WHEN TRIM(UPPER(MARCA)) IN ('BONAFONT SER','SER PURA') THEN 'BONAFONT SER'
            WHEN TRIM(UPPER(MARCA)) IN ('TE BONAFONT','TÉ BONAFONT','BONAFONT TE','BONAFONT_TE','BONAFONT TÉ') THEN 'BONAFONT TE'
            WHEN TRIM(UPPER(MARCA)) IN ('DANETTE') THEN 'DANETTE'
            WHEN TRIM(UPPER(MARCA)) IN ('DANMIX','DAN MIX','DANMMIX') THEN 'DANMIX'
            WHEN TRIM(UPPER(MARCA)) IN ('DANONE','DANONE YOGHURT','DANONE CREME','DANONE FREE','DANONE GRIEGO','DANONE FS','DAIRY') THEN 'DANONE'
            WHEN TRIM(UPPER(MARCA)) IN ('DANONINO','DANONINOLIQUIDO') THEN 'DANONINO'
            WHEN TRIM(UPPER(MARCA)) IN ('DANUP','DAN UP') THEN 'DANUP'
            WHEN TRIM(UPPER(MARCA)) IN ('DANY','DANY DANETTE') THEN 'DANY'
            WHEN TRIM(UPPER(MARCA)) IN ('DELIGHT') THEN 'DELIGHT'
            WHEN TRIM(UPPER(MARCA)) IN ('EVIAN') THEN 'EVIAN'
            WHEN TRIM(UPPER(MARCA)) IN ('HERSHEYS','HERSHEY''S','DANONE HERSHEYS') THEN 'HERSHEYS'
            WHEN TRIM(UPPER(MARCA)) IN ('INFINIT WATER','INFINIT','INFINITY','WATER INFINIT') THEN 'INFINIT'
            WHEN TRIM(UPPER(MARCA)) IN ('JUIZZY') THEN 'JUIZZY'
            WHEN TRIM(UPPER(MARCA)) IN ('LEVITE','BONAFONT_LEVITE','BONAFONT LEVITE') THEN 'LEVITE'
            WHEN TRIM(UPPER(MARCA)) IN ('LICUAMIX') THEN 'LICUAMIX'
            WHEN TRIM(UPPER(MARCA)) IN ('OCEAN SPRAY','OCEAN') THEN 'OCEAN SPRAY'
            WHEN TRIM(UPPER(MARCA)) IN ('OIKOS','OIKOS UHT') THEN 'OIKOS'
            WHEN TRIM(UPPER(MARCA)) IN ('SILK','SILK ORIG 946ML','SILKCHOCO190ML') THEN 'SILK'
            WHEN TRIM(UPPER(MARCA)) IN ('SO DELICIOUS') THEN 'SO DELICIOUS'
            WHEN TRIM(UPPER(MARCA)) IN ('STOK COLD BREW','STOK') THEN 'STOK'
            WHEN TRIM(UPPER(MARCA)) IN ('VITALINEA') THEN 'VITALINEA'
            WHEN TRIM(UPPER(MARCA)) IN ('YOPRO','YO PRO') THEN 'YOPRO'
            ELSE TRIM(UPPER(MARCA))
        END AS MARCA_STD
    FROM PRD_MDP.MDP_DSP.VW_MKT_ECOMM
    WHERE ANIO >= 2024
      AND TRIM(UPPER(MARCA)) NOT IN ('MULTIBRAND','MULTIBRAND DAIRY','MULTIBRAND DANONE',
                                      'MULTIBRAND INDULGENCE','MULTIBRAND INNOS','MULTIBRAND KIDS',
                                      'MULTIBRAND WATERS','MULTIMARCA','INSTITUTO DANONE','DNP','DANONE FS',
                                      '_MERCHANDISE','_UNKNOWN','_MULTIBRAND')
),
overlap AS (
    SELECT
        COUNT(DISTINCT so.MARCA_STD)                                               AS SO_DANONE_BRANDS,
        COUNT(DISTINCT CASE WHEN mo.MARCA_STD IS NOT NULL THEN so.MARCA_STD END)   AS MATCHED_BRANDS,
        COUNT(DISTINCT CASE WHEN mo.MARCA_STD IS NULL     THEN so.MARCA_STD END)   AS UNMATCHED_BRANDS,
        COUNT(DISTINCT CASE WHEN mo.MARCA_STD IS NOT NULL THEN so.MARCA_STD END)
            * 100.0 / NULLIF(COUNT(DISTINCT so.MARCA_STD), 0)                     AS MATCH_PCT
    FROM sell_out_brands so
    LEFT JOIN mkt_on_brands mo ON so.MARCA_STD = mo.MARCA_STD
)
SELECT * FROM overlap
"""

try:
    df_v13a, _ = run_sf_query("PRD_MDP", v13a_sql, "V13A: SELL_OUT × MKT_ON brand overlap")
    log_df(df_v13a, "V13A overlap summary", n=5)
    row_a     = df_v13a.collect()[0]
    match_pct = float(row_a["MATCH_PCT"] or 0.0)
    log(f"  V13A — SO Danone brands: {row_a['SO_DANONE_BRANDS']}  "
        f"Matched: {row_a['MATCHED_BRANDS']}  "
        f"Unmatched: {row_a['UNMATCHED_BRANDS']}  "
        f"Match%: {match_pct:.1f}%")
    if match_pct < 70.0:
        warn(f"V13A: SELL_OUT × MKT_ON match = {match_pct:.1f}% (threshold: 70%). "
             f"{row_a['UNMATCHED_BRANDS']} Danone brand(s) in SELL_OUT with no MKT_ON data.")
    else:
        log(f"  ✅ V13A PASS — match {match_pct:.1f}% >= 70%")
except Exception as e:
    blocker(f"V13A query failed: {e}")

abort_if_blockers()

# COMMAND ----------

# MAGIC %md
# MAGIC ## V13B — SELL_OUT × MKT_OFF brand match (Danone brands only, warn if < 70%)

# COMMAND ----------

log("=" * 70)
log("V13B — SELL_OUT × MKT_OFF: Danone brand overlap (Danone only)")
log("Threshold: WARN if < 70% of SELL_OUT Danone brands matched in MKT_OFF")
log("=" * 70)

v13b_sql = f"""
WITH sell_out_danone AS (
    SELECT DISTINCT
        CASE
            WHEN TRIM(UPPER(prod.BRAND)) IN ('ACTIVIA') THEN 'ACTIVIA'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('AGUAS FRESCAS','BFT AGUAS FRESCAS','BONAFONT_AGUASFRESCAS','BONAFONT AGUA FRESCAS') THEN 'AGUAS FRESCAS'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('BENEGASTRO','BENEG') THEN 'BENEGASTRO'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('BONAFONT','BONAFONT_NATURAL','AZUL BONAFONT','BONAFONT NATURAL','WATER BONAFONT','BONAFONT 11 LTS') THEN 'BONAFONT'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('BONAFONT KIDS','BONAFONT_KIDS','BONAFONT KIDS NATURAL','Kids','KIDS') THEN 'BONAFONT KIDS'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('BONAFONT SER','SER PURA') THEN 'BONAFONT SER'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('TE BONAFONT','TÉ BONAFONT','BONAFONT TE','BONAFONT_TE','BONAFONT TÉ') THEN 'BONAFONT TE'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('DANETTE') THEN 'DANETTE'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('DANMIX','DAN MIX','DANMMIX') THEN 'DANMIX'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('DANONE','DANONE YOGHURT','DANONE CREME','DANONE FREE','DANONE GRIEGO','DANONE FS','DAIRY','Danone','DAIRY DANONE MEXICO','DANAO') THEN 'DANONE'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('DANONINO','DANONINOLIQUIDO') THEN 'DANONINO'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('DANUP','DAN UP','DAN''UP') THEN 'DANUP'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('DANY','DANY DANETTE') THEN 'DANY'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('DELIGHT') THEN 'DELIGHT'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('EVIAN','FERRARELLE') THEN 'EVIAN'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('HERSHEYS','HERSHEY''S','DANONE HERSHEYS') THEN 'HERSHEYS'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('INFINIT WATER','INFINIT','INFINITY','WATER INFINIT') THEN 'INFINIT'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('JUIZZY','Juizzy','FRUIX') THEN 'JUIZZY'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('LEVITE','BONAFONT_LEVITE','BONAFONT LEVITE','Levite','LEVITE CLASICA','LEVITE INFUSIONES','LEVITE CERO','LEVITE BALANCE') THEN 'LEVITE'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('LICUAMIX') THEN 'LICUAMIX'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('OCEAN SPRAY','OCEAN') THEN 'OCEAN SPRAY'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('OIKOS','OIKOS UHT') THEN 'OIKOS'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('PUREZA AGA','AGA','AGA 20 LTS','AQUAPURA','AGUA NATURAL','Aga','Aga 20 Lts','Agua Natural','Ultrapura','ULTRAPURA','Botella Aga','AGA PUREZA') THEN 'PUREZA AGA'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('SILK','SILK ORIG 946ML','SILKCHOCO190ML','Silk') THEN 'SILK'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('SO DELICIOUS') THEN 'SO DELICIOUS'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('STOK COLD BREW','STOK') THEN 'STOK'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('VITALINEA') THEN 'VITALINEA'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('YOPRO','YO PRO') THEN 'YOPRO'
            ELSE TRIM(UPPER(prod.BRAND))
        END AS MARCA_STD
    FROM PRD_MDP.MDP_DSP.VW_FACT_SELL_OUT f
    INNER JOIN PRD_MDP.MDP_DWH.V_D_PERIOD per ON f.PER_ID = per.PER_ID
    INNER JOIN PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM prod
        ON TO_VARCHAR(f.UPC) = TO_VARCHAR(prod.INT_ID)
        AND f.CBU_ID = prod.CBU_ID
    QUALIFY
        CASE
            WHEN TRIM(UPPER(prod.BRAND)) IN ('ACTIVIA') THEN 'ACTIVIA'
            ELSE TRIM(UPPER(prod.BRAND))
        END IN {DANONE_IN_CLAUSE}
),
mkt_off_danone AS (
    -- MKT_OFF: Danone-only rows (confirmed — competitors excluded from mart)
    SELECT DISTINCT
        CASE
            WHEN TRIM(UPPER(MARCA)) IN ('ACTIVIA') THEN 'ACTIVIA'
            WHEN TRIM(UPPER(MARCA)) IN ('AGUAS FRESCAS','BFT AGUAS FRESCAS','BONAFONT_AGUASFRESCAS','BONAFONT AGUA FRESCAS') THEN 'AGUAS FRESCAS'
            WHEN TRIM(UPPER(MARCA)) IN ('BENEGASTRO','BENEG') THEN 'BENEGASTRO'
            WHEN TRIM(UPPER(MARCA)) IN ('BONAFONT','BONAFONT_NATURAL','AZUL BONAFONT') THEN 'BONAFONT'
            WHEN TRIM(UPPER(MARCA)) IN ('BONAFONT KIDS','BONAFONT_KIDS','BONAFONT KIDS NATURAL') THEN 'BONAFONT KIDS'
            WHEN TRIM(UPPER(MARCA)) IN ('BONAFONT SER','SER PURA') THEN 'BONAFONT SER'
            WHEN TRIM(UPPER(MARCA)) IN ('TE BONAFONT','TÉ BONAFONT','BONAFONT TE','BONAFONT_TE','BONAFONT TÉ') THEN 'BONAFONT TE'
            WHEN TRIM(UPPER(MARCA)) IN ('DANETTE') THEN 'DANETTE'
            WHEN TRIM(UPPER(MARCA)) IN ('DANMIX','DAN MIX','DANMMIX') THEN 'DANMIX'
            WHEN TRIM(UPPER(MARCA)) IN ('DANONE','DANONE YOGHURT','DANONE CREME','DANONE FREE','DANONE GRIEGO','DANONE FS','DAIRY') THEN 'DANONE'
            WHEN TRIM(UPPER(MARCA)) IN ('DANONINO','DANONINOLIQUIDO') THEN 'DANONINO'
            WHEN TRIM(UPPER(MARCA)) IN ('DANUP','DAN UP') THEN 'DANUP'
            WHEN TRIM(UPPER(MARCA)) IN ('DANY','DANY DANETTE') THEN 'DANY'
            WHEN TRIM(UPPER(MARCA)) IN ('DELIGHT') THEN 'DELIGHT'
            WHEN TRIM(UPPER(MARCA)) IN ('EVIAN') THEN 'EVIAN'
            WHEN TRIM(UPPER(MARCA)) IN ('HERSHEYS','HERSHEY''S','DANONE HERSHEYS') THEN 'HERSHEYS'
            WHEN TRIM(UPPER(MARCA)) IN ('INFINIT WATER','INFINIT','INFINITY','WATER INFINIT') THEN 'INFINIT'
            WHEN TRIM(UPPER(MARCA)) IN ('LEVITE','BONAFONT_LEVITE','BONAFONT LEVITE') THEN 'LEVITE'
            WHEN TRIM(UPPER(MARCA)) IN ('OCEAN SPRAY','OCEAN') THEN 'OCEAN SPRAY'
            WHEN TRIM(UPPER(MARCA)) IN ('OIKOS','OIKOS UHT') THEN 'OIKOS'
            WHEN TRIM(UPPER(MARCA)) IN ('SILK','SILK ORIG 946ML','SILKCHOCO190ML') THEN 'SILK'
            WHEN TRIM(UPPER(MARCA)) IN ('SO DELICIOUS') THEN 'SO DELICIOUS'
            WHEN TRIM(UPPER(MARCA)) IN ('STOK COLD BREW','STOK') THEN 'STOK'
            WHEN TRIM(UPPER(MARCA)) IN ('VITALINEA') THEN 'VITALINEA'
            WHEN TRIM(UPPER(MARCA)) IN ('YOPRO','YO PRO') THEN 'YOPRO'
            ELSE TRIM(UPPER(MARCA))
        END AS MARCA_STD
    FROM PRD_MDP.MDP_STG.FACT_MEDIA_OFF
    WHERE ANIO >= 2024
      AND INVERSION_REAL IS NOT NULL
      AND TRIM(UPPER(MARCA)) NOT IN
          ('COCA COLA','COCA-COLA','COCACOLA','THE COCA COLA EXPORT','COCA COLA FEMSA',
           'PEPSI','PEPSI COLA MEXICANA','PEPSICOLA MEXICANA','LALA','GPO INDUSTRIAL LALA',
           'ALPURA','YOPLAIT','JUMEX','SANTA CLARA','CIEL','PENAFIEL','SEVEN UP','SEVENUP',
           '_MERCHANDISE','_UNKNOWN','_MULTIBRAND','MULTIBRAND','MULTIMARCA')
),
overlap AS (
    SELECT
        COUNT(DISTINCT so.MARCA_STD)                                               AS SO_DANONE_BRANDS,
        COUNT(DISTINCT CASE WHEN mo.MARCA_STD IS NOT NULL THEN so.MARCA_STD END)   AS MATCHED_BRANDS,
        COUNT(DISTINCT CASE WHEN mo.MARCA_STD IS NULL     THEN so.MARCA_STD END)   AS UNMATCHED_BRANDS,
        COUNT(DISTINCT CASE WHEN mo.MARCA_STD IS NOT NULL THEN so.MARCA_STD END)
            * 100.0 / NULLIF(COUNT(DISTINCT so.MARCA_STD), 0)                     AS MATCH_PCT
    FROM sell_out_danone so
    LEFT JOIN mkt_off_danone mo ON so.MARCA_STD = mo.MARCA_STD
)
SELECT * FROM overlap
"""

try:
    df_v13b, _ = run_sf_query("PRD_MDP", v13b_sql, "V13B: SELL_OUT × MKT_OFF brand overlap (Danone only)")
    log_df(df_v13b, "V13B overlap summary", n=5)
    row_b     = df_v13b.collect()[0]
    match_pct = float(row_b["MATCH_PCT"] or 0.0)
    log(f"  V13B — SO Danone brands: {row_b['SO_DANONE_BRANDS']}  "
        f"Matched: {row_b['MATCHED_BRANDS']}  "
        f"Unmatched: {row_b['UNMATCHED_BRANDS']}  "
        f"Match%: {match_pct:.1f}%")
    if match_pct < 70.0:
        warn(f"V13B: SELL_OUT × MKT_OFF match = {match_pct:.1f}% (threshold: 70%). "
             f"Expected: some Danone brands have no OFF media spend (confirmed acceptable).")
    else:
        log(f"  ✅ V13B PASS — match {match_pct:.1f}% >= 70%")
except Exception as e:
    blocker(f"V13B query failed: {e}")

abort_if_blockers()

# COMMAND ----------

# MAGIC %md
# MAGIC ## V13C — SELL_OUT × WASTE brand match (warn if < 80%)

# COMMAND ----------

log("=" * 70)
log("V13C — SELL_OUT × WASTE: brand overlap at MARCA_STD grain")
log("Threshold: WARN if < 80% of SELL_OUT brands matched in WASTE")
log("=" * 70)

v13c_sql = """
WITH sell_out_brands AS (
    SELECT DISTINCT
        CASE
            WHEN TRIM(UPPER(prod.BRAND)) IN ('ACTIVIA') THEN 'ACTIVIA'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('AGUAS FRESCAS','BFT AGUAS FRESCAS','BONAFONT_AGUASFRESCAS','BONAFONT AGUA FRESCAS') THEN 'AGUAS FRESCAS'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('BENEGASTRO','BENEG') THEN 'BENEGASTRO'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('BONAFONT','BONAFONT_NATURAL','AZUL BONAFONT','BONAFONT NATURAL','WATER BONAFONT','BONAFONT 11 LTS') THEN 'BONAFONT'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('BONAFONT KIDS','BONAFONT_KIDS','BONAFONT KIDS NATURAL','Kids','KIDS') THEN 'BONAFONT KIDS'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('BONAFONT SER','SER PURA') THEN 'BONAFONT SER'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('TE BONAFONT','TÉ BONAFONT','BONAFONT TE','BONAFONT_TE','BONAFONT TÉ') THEN 'BONAFONT TE'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('DANETTE') THEN 'DANETTE'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('DANMIX','DAN MIX','DANMMIX') THEN 'DANMIX'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('DANONE','DANONE YOGHURT','DANONE CREME','DANONE FREE','DANONE GRIEGO','DANONE FS','DAIRY','Danone','DAIRY DANONE MEXICO','DANAO') THEN 'DANONE'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('DANONINO','DANONINOLIQUIDO') THEN 'DANONINO'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('DANUP','DAN UP','DAN''UP') THEN 'DANUP'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('DANY','DANY DANETTE') THEN 'DANY'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('DELIGHT') THEN 'DELIGHT'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('EVIAN','FERRARELLE') THEN 'EVIAN'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('HERSHEYS','HERSHEY''S','DANONE HERSHEYS') THEN 'HERSHEYS'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('INFINIT WATER','INFINIT','INFINITY','WATER INFINIT') THEN 'INFINIT'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('JUIZZY','Juizzy','FRUIX') THEN 'JUIZZY'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('LEVITE','BONAFONT_LEVITE','BONAFONT LEVITE','Levite','LEVITE CLASICA','LEVITE INFUSIONES','LEVITE CERO','LEVITE BALANCE') THEN 'LEVITE'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('LICUAMIX') THEN 'LICUAMIX'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('OCEAN SPRAY','OCEAN') THEN 'OCEAN SPRAY'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('OIKOS','OIKOS UHT') THEN 'OIKOS'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('PUREZA AGA','AGA','AGA 20 LTS','AQUAPURA','AGUA NATURAL','Aga','Aga 20 Lts','Agua Natural','Ultrapura','ULTRAPURA','Botella Aga','AGA PUREZA') THEN 'PUREZA AGA'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('SILK','SILK ORIG 946ML','SILKCHOCO190ML','Silk') THEN 'SILK'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('SO DELICIOUS') THEN 'SO DELICIOUS'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('STOK COLD BREW','STOK') THEN 'STOK'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('VITALINEA') THEN 'VITALINEA'
            WHEN TRIM(UPPER(prod.BRAND)) IN ('YOPRO','YO PRO') THEN 'YOPRO'
            ELSE TRIM(UPPER(prod.BRAND))
        END AS MARCA_STD
    FROM PRD_MDP.MDP_DSP.VW_FACT_SELL_OUT f
    INNER JOIN PRD_MDP.MDP_DWH.V_D_PERIOD per ON f.PER_ID = per.PER_ID
    INNER JOIN PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM prod
        ON TO_VARCHAR(f.UPC) = TO_VARCHAR(prod.INT_ID)
        AND f.CBU_ID = prod.CBU_ID
),
waste_brands AS (
    SELECT DISTINCT
        CASE
            WHEN TRIM(UPPER(MARCA)) IN ('ACTIVIA') THEN 'ACTIVIA'
            WHEN TRIM(UPPER(MARCA)) IN ('AGUAS FRESCAS','BFT AGUAS FRESCAS','BONAFONT_AGUASFRESCAS','BONAFONT AGUA FRESCAS') THEN 'AGUAS FRESCAS'
            WHEN TRIM(UPPER(MARCA)) IN ('BENEGASTRO','BENEG') THEN 'BENEGASTRO'
            WHEN TRIM(UPPER(MARCA)) IN ('BONAFONT','BONAFONT_NATURAL','AZUL BONAFONT') THEN 'BONAFONT'
            WHEN TRIM(UPPER(MARCA)) IN ('BONAFONT KIDS','BONAFONT_KIDS','BONAFONT KIDS NATURAL') THEN 'BONAFONT KIDS'
            WHEN TRIM(UPPER(MARCA)) IN ('BONAFONT SER','SER PURA') THEN 'BONAFONT SER'
            WHEN TRIM(UPPER(MARCA)) IN ('TE BONAFONT','TÉ BONAFONT','BONAFONT TE','BONAFONT_TE','BONAFONT TÉ') THEN 'BONAFONT TE'
            WHEN TRIM(UPPER(MARCA)) IN ('DANETTE') THEN 'DANETTE'
            WHEN TRIM(UPPER(MARCA)) IN ('DANMIX','DAN MIX','DANMMIX') THEN 'DANMIX'
            WHEN TRIM(UPPER(MARCA)) IN ('DANONE','DANONE YOGHURT','DANONE CREME','DANONE FREE','DANONE GRIEGO','DANONE FS','DAIRY') THEN 'DANONE'
            WHEN TRIM(UPPER(MARCA)) IN ('DANONINO','DANONINOLIQUIDO') THEN 'DANONINO'
            WHEN TRIM(UPPER(MARCA)) IN ('DANUP','DAN UP') THEN 'DANUP'
            WHEN TRIM(UPPER(MARCA)) IN ('DANY','DANY DANETTE') THEN 'DANY'
            WHEN TRIM(UPPER(MARCA)) IN ('DELIGHT') THEN 'DELIGHT'
            WHEN TRIM(UPPER(MARCA)) IN ('EVIAN') THEN 'EVIAN'
            WHEN TRIM(UPPER(MARCA)) IN ('HERSHEYS','HERSHEY''S','DANONE HERSHEYS') THEN 'HERSHEYS'
            WHEN TRIM(UPPER(MARCA)) IN ('JUIZZY','Juizzy') THEN 'JUIZZY'
            WHEN TRIM(UPPER(MARCA)) IN ('LEVITE','BONAFONT_LEVITE','BONAFONT LEVITE') THEN 'LEVITE'
            WHEN TRIM(UPPER(MARCA)) IN ('LICUAMIX') THEN 'LICUAMIX'
            WHEN TRIM(UPPER(MARCA)) IN ('OCEAN SPRAY','OCEAN') THEN 'OCEAN SPRAY'
            WHEN TRIM(UPPER(MARCA)) IN ('OIKOS','OIKOS UHT') THEN 'OIKOS'
            WHEN TRIM(UPPER(MARCA)) IN ('SILK','SILK ORIG 946ML','SILKCHOCO190ML') THEN 'SILK'
            WHEN TRIM(UPPER(MARCA)) IN ('SO DELICIOUS') THEN 'SO DELICIOUS'
            WHEN TRIM(UPPER(MARCA)) IN ('STOK COLD BREW','STOK') THEN 'STOK'
            WHEN TRIM(UPPER(MARCA)) IN ('VITALINEA') THEN 'VITALINEA'
            WHEN TRIM(UPPER(MARCA)) IN ('YOPRO','YO PRO') THEN 'YOPRO'
            ELSE TRIM(UPPER(MARCA))
        END AS MARCA_STD
    FROM PRD_MDP.MDP_STG.VW_WASTE
    WHERE MARCA IS NOT NULL
      AND TRIM(UPPER(MARCA)) NOT IN ('0','MULTI','_UNKNOWN')
),
overlap AS (
    SELECT
        COUNT(DISTINCT so.MARCA_STD)                                              AS SO_BRANDS,
        COUNT(DISTINCT CASE WHEN w.MARCA_STD IS NOT NULL THEN so.MARCA_STD END)   AS MATCHED_BRANDS,
        COUNT(DISTINCT CASE WHEN w.MARCA_STD IS NULL     THEN so.MARCA_STD END)   AS UNMATCHED_BRANDS,
        COUNT(DISTINCT CASE WHEN w.MARCA_STD IS NOT NULL THEN so.MARCA_STD END)
            * 100.0 / NULLIF(COUNT(DISTINCT so.MARCA_STD), 0)                    AS MATCH_PCT
    FROM sell_out_brands so
    LEFT JOIN waste_brands w ON so.MARCA_STD = w.MARCA_STD
)
SELECT * FROM overlap
"""

try:
    df_v13c, _ = run_sf_query("PRD_MDP", v13c_sql, "V13C: SELL_OUT × WASTE brand overlap")
    log_df(df_v13c, "V13C overlap summary", n=5)
    row_c     = df_v13c.collect()[0]
    match_pct = float(row_c["MATCH_PCT"] or 0.0)
    log(f"  V13C — SO brands: {row_c['SO_BRANDS']}  "
        f"Matched: {row_c['MATCHED_BRANDS']}  "
        f"Unmatched: {row_c['UNMATCHED_BRANDS']}  "
        f"Match%: {match_pct:.1f}%")
    if match_pct < 80.0:
        warn(f"V13C: SELL_OUT × WASTE match = {match_pct:.1f}% (threshold: 80%). "
             f"{row_c['UNMATCHED_BRANDS']} brand(s) in SELL_OUT with no WASTE record.")
    else:
        log(f"  ✅ V13C PASS — match {match_pct:.1f}% >= 80%")
except Exception as e:
    blocker(f"V13C query failed: {e}")

abort_if_blockers()

# COMMAND ----------

# MAGIC %md
# MAGIC ## V13D — SELL_IN × IBP brand match (warn if < 80%)
# MAGIC Note: SELL_IN is in PRD_MEX; IBP is in PRD_MDP. Queried separately, joined in Python.

# COMMAND ----------

log("=" * 70)
log("V13D — SELL_IN × IBP: brand overlap at MARCA_STD grain")
log("Note: Cross-DB join — SELL_IN (PRD_MEX) vs IBP (PRD_MDP)")
log("Threshold: WARN if < 80% of SELL_IN brands matched in IBP")
log("=" * 70)

sell_in_brands_sql = """
SELECT DISTINCT
    CASE
        WHEN TRIM(UPPER(PRO.LV2_UMB_BRD_DSC)) IN ('ACTIVIA') THEN 'ACTIVIA'
        WHEN TRIM(UPPER(PRO.LV2_UMB_BRD_DSC)) IN ('AGUAS FRESCAS','BFT AGUAS FRESCAS','BONAFONT_AGUASFRESCAS','BONAFONT AGUA FRESCAS') THEN 'AGUAS FRESCAS'
        WHEN TRIM(UPPER(PRO.LV2_UMB_BRD_DSC)) IN ('BENEGASTRO','BENEG') THEN 'BENEGASTRO'
        WHEN TRIM(UPPER(PRO.LV2_UMB_BRD_DSC)) IN ('BONAFONT','BONAFONT_NATURAL','AZUL BONAFONT','BONAFONT NATURAL','WATER BONAFONT') THEN 'BONAFONT'
        WHEN TRIM(UPPER(PRO.LV2_UMB_BRD_DSC)) IN ('BONAFONT JUGO') THEN 'BONAFONT JUGO'
        WHEN TRIM(UPPER(PRO.LV2_UMB_BRD_DSC)) IN ('BONAFONT KIDS','BONAFONT_KIDS','BONAFONT KIDS NATURAL','Kids','KIDS') THEN 'BONAFONT KIDS'
        WHEN TRIM(UPPER(PRO.LV2_UMB_BRD_DSC)) IN ('BONAFONT MINERAL','MINERALIZADA','Mineralizada') THEN 'BONAFONT MINERAL'
        WHEN TRIM(UPPER(PRO.LV2_UMB_BRD_DSC)) IN ('BONAFONT SER','SER PURA') THEN 'BONAFONT SER'
        WHEN TRIM(UPPER(PRO.LV2_UMB_BRD_DSC)) IN ('TE BONAFONT','TÉ BONAFONT','BONAFONT TE','BONAFONT_TE','BONAFONT TÉ') THEN 'BONAFONT TE'
        WHEN TRIM(UPPER(PRO.LV2_UMB_BRD_DSC)) IN ('DANETTE') THEN 'DANETTE'
        WHEN TRIM(UPPER(PRO.LV2_UMB_BRD_DSC)) IN ('DANMIX','DAN MIX','DANMMIX') THEN 'DANMIX'
        WHEN TRIM(UPPER(PRO.LV2_UMB_BRD_DSC)) IN ('DANONE','DANONE YOGHURT','DANONE CREME','DANONE FREE','DANONE GRIEGO','DANONE FS','DAIRY','Danone','DAIRY DANONE MEXICO','DANAO') THEN 'DANONE'
        WHEN TRIM(UPPER(PRO.LV2_UMB_BRD_DSC)) IN ('DANONINO','DANONINOLIQUIDO') THEN 'DANONINO'
        WHEN TRIM(UPPER(PRO.LV2_UMB_BRD_DSC)) IN ('DANUP','DAN UP') THEN 'DANUP'
        WHEN TRIM(UPPER(PRO.LV2_UMB_BRD_DSC)) IN ('DANY','DANY DANETTE') THEN 'DANY'
        WHEN TRIM(UPPER(PRO.LV2_UMB_BRD_DSC)) IN ('DELIGHT') THEN 'DELIGHT'
        WHEN TRIM(UPPER(PRO.LV2_UMB_BRD_DSC)) IN ('EVIAN','FERRARELLE') THEN 'EVIAN'
        WHEN TRIM(UPPER(PRO.LV2_UMB_BRD_DSC)) IN ('HERSHEYS','HERSHEY''S','DANONE HERSHEYS') THEN 'HERSHEYS'
        WHEN TRIM(UPPER(PRO.LV2_UMB_BRD_DSC)) IN ('INFINIT WATER','INFINIT','INFINITY','WATER INFINIT') THEN 'INFINIT'
        WHEN TRIM(UPPER(PRO.LV2_UMB_BRD_DSC)) IN ('JUIZZY','Juizzy','FRUIX') THEN 'JUIZZY'
        WHEN TRIM(UPPER(PRO.LV2_UMB_BRD_DSC)) IN ('LEVITE','BONAFONT_LEVITE','BONAFONT LEVITE','Levite','LEVITE CLASICA','LEVITE INFUSIONES','LEVITE CERO','LEVITE BALANCE') THEN 'LEVITE'
        WHEN TRIM(UPPER(PRO.LV2_UMB_BRD_DSC)) IN ('LICUAMIX') THEN 'LICUAMIX'
        WHEN TRIM(UPPER(PRO.LV2_UMB_BRD_DSC)) IN ('OCEAN SPRAY','OCEAN') THEN 'OCEAN SPRAY'
        WHEN TRIM(UPPER(PRO.LV2_UMB_BRD_DSC)) IN ('OIKOS','OIKOS UHT') THEN 'OIKOS'
        WHEN TRIM(UPPER(PRO.LV2_UMB_BRD_DSC)) IN ('SILK','SILK ORIG 946ML','SILKCHOCO190ML','Silk') THEN 'SILK'
        WHEN TRIM(UPPER(PRO.LV2_UMB_BRD_DSC)) IN ('SO DELICIOUS') THEN 'SO DELICIOUS'
        WHEN TRIM(UPPER(PRO.LV2_UMB_BRD_DSC)) IN ('STOK COLD BREW','STOK') THEN 'STOK'
        WHEN TRIM(UPPER(PRO.LV2_UMB_BRD_DSC)) IN ('VITALINEA') THEN 'VITALINEA'
        WHEN TRIM(UPPER(PRO.LV2_UMB_BRD_DSC)) IN ('YOPRO','YO PRO') THEN 'YOPRO'
        ELSE TRIM(UPPER(PRO.LV2_UMB_BRD_DSC))
    END AS MARCA_STD
FROM PRD_MEX.MEX_DSP_OTC.VW_FACT_RNV FAC
INNER JOIN PRD_MEX.MEX_DSP_OTC.V_D_ITEM PRO ON FAC.MAT_IDT = PRO.MAT_IDT
WHERE PRO.LV2_UMB_BRD_DSC IS NOT NULL
  AND TRIM(UPPER(PRO.LV2_UMB_BRD_DSC)) NOT IN
      ('_MERCHANDISE','_UNKNOWN','LONCHERA','PALAS','TABLAS','TAZAS','VASOS','TOALLAS',
       'TUPPER','UTENSILIOS','CERAMICA','VAJILLA','LAPICERA','COSMETIQUERA','PLAYERA MUNDIAL',
       'PORTAVASOS','LIBRO NAVIDEÑO','REFRACTARIOS','BOTANERO','DISPENSERS','BEBEDERO',
       'BOMBA','JARRA','TERMO','ALCANCIA','METALICO','ESPECIERO','POUCH','BOWLS',
       'CODISTRIBUCION','OTHERS','PRIVADA','PROMO','Promo','KINDER')
"""

ibp_brands_sql = """
SELECT DISTINCT
    CASE
        WHEN TRIM(UPPER(MARCA)) IN ('ACTIVIA') THEN 'ACTIVIA'
        WHEN TRIM(UPPER(MARCA)) IN ('AGUAS FRESCAS','BFT AGUAS FRESCAS','BONAFONT_AGUASFRESCAS','BONAFONT AGUA FRESCAS') THEN 'AGUAS FRESCAS'
        WHEN TRIM(UPPER(MARCA)) IN ('BENEGASTRO','BENEG') THEN 'BENEGASTRO'
        WHEN TRIM(UPPER(MARCA)) IN ('BONAFONT','BONAFONT_NATURAL','AZUL BONAFONT') THEN 'BONAFONT'
        WHEN TRIM(UPPER(MARCA)) IN ('BONAFONT KIDS','BONAFONT_KIDS','BONAFONT KIDS NATURAL') THEN 'BONAFONT KIDS'
        WHEN TRIM(UPPER(MARCA)) IN ('BONAFONT SER','SER PURA') THEN 'BONAFONT SER'
        WHEN TRIM(UPPER(MARCA)) IN ('TE BONAFONT','TÉ BONAFONT','BONAFONT TE','BONAFONT_TE','BONAFONT TÉ') THEN 'BONAFONT TE'
        WHEN TRIM(UPPER(MARCA)) IN ('DANETTE') THEN 'DANETTE'
        WHEN TRIM(UPPER(MARCA)) IN ('DANMIX','DAN MIX','DANMMIX') THEN 'DANMIX'
        WHEN TRIM(UPPER(MARCA)) IN ('DANONE','DANONE YOGHURT','DANONE CREME','DANONE FREE','DANONE GRIEGO','DANONE FS','DAIRY') THEN 'DANONE'
        WHEN TRIM(UPPER(MARCA)) IN ('DANONINO','DANONINOLIQUIDO') THEN 'DANONINO'
        WHEN TRIM(UPPER(MARCA)) IN ('DANUP','DAN UP') THEN 'DANUP'
        WHEN TRIM(UPPER(MARCA)) IN ('DANY','DANY DANETTE') THEN 'DANY'
        WHEN TRIM(UPPER(MARCA)) IN ('DELIGHT') THEN 'DELIGHT'
        WHEN TRIM(UPPER(MARCA)) IN ('EVIAN') THEN 'EVIAN'
        WHEN TRIM(UPPER(MARCA)) IN ('HERSHEYS','HERSHEY''S','DANONE HERSHEYS') THEN 'HERSHEYS'
        WHEN TRIM(UPPER(MARCA)) IN ('INFINIT WATER','INFINIT','INFINITY','WATER INFINIT') THEN 'INFINIT'
        WHEN TRIM(UPPER(MARCA)) IN ('JUIZZY') THEN 'JUIZZY'
        WHEN TRIM(UPPER(MARCA)) IN ('LEVITE','BONAFONT_LEVITE','BONAFONT LEVITE') THEN 'LEVITE'
        WHEN TRIM(UPPER(MARCA)) IN ('OCEAN SPRAY','OCEAN') THEN 'OCEAN SPRAY'
        WHEN TRIM(UPPER(MARCA)) IN ('OIKOS','OIKOS UHT') THEN 'OIKOS'
        WHEN TRIM(UPPER(MARCA)) IN ('SILK','SILK ORIG 946ML','SILKCHOCO190ML') THEN 'SILK'
        WHEN TRIM(UPPER(MARCA)) IN ('SO DELICIOUS') THEN 'SO DELICIOUS'
        WHEN TRIM(UPPER(MARCA)) IN ('STOK COLD BREW','STOK') THEN 'STOK'
        WHEN TRIM(UPPER(MARCA)) IN ('VITALINEA') THEN 'VITALINEA'
        WHEN TRIM(UPPER(MARCA)) IN ('YOPRO','YO PRO') THEN 'YOPRO'
        ELSE TRIM(UPPER(MARCA))
    END AS MARCA_STD
FROM PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP
WHERE MARCA IS NOT NULL
"""

try:
    df_si, _ = run_sf_query("PRD_MEX", sell_in_brands_sql, "V13D: SELL_IN distinct MARCA_STD")
    df_ibp, _= run_sf_query("PRD_MDP", ibp_brands_sql,    "V13D: IBP distinct MARCA_STD")

    # Cross-DB join in Python (ISSUE-003: PRD_MEX + PRD_MDP cannot share one SF session)
    si_brands  = {r["MARCA_STD"] for r in df_si.collect()  if r["MARCA_STD"]}
    ibp_brands = {r["MARCA_STD"] for r in df_ibp.collect() if r["MARCA_STD"]}

    matched   = si_brands & ibp_brands
    unmatched = si_brands - ibp_brands
    match_pct = len(matched) * 100.0 / max(len(si_brands), 1)

    log(f"  V13D — SI brands: {len(si_brands)}  "
        f"IBP brands: {len(ibp_brands)}  "
        f"Matched: {len(matched)}  "
        f"Unmatched: {len(unmatched)}  "
        f"Match%: {match_pct:.1f}%")

    if unmatched:
        log(f"  Unmatched SELL_IN brands (no IBP match): {sorted(unmatched)}")

    if match_pct < 80.0:
        warn(f"V13D: SELL_IN × IBP match = {match_pct:.1f}% (threshold: 80%). "
             f"Unmatched: {sorted(unmatched)}")
    else:
        log(f"  ✅ V13D PASS — match {match_pct:.1f}% >= 80%")

except Exception as e:
    blocker(f"V13D query failed: {e}")

abort_if_blockers()

# COMMAND ----------

# MAGIC %md
# MAGIC ## V13E — SELL_OUT × EDP Nielsen brand match (warn if < 60%)
# MAGIC Source: PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_PROD_DIM (INP_56985 = brand)

# COMMAND ----------

log("=" * 70)
log("V13E — SELL_OUT × EDP Nielsen: brand overlap at MARCA_STD grain")
log("Threshold: WARN if < 60% of SELL_OUT brands matched in EDP Nielsen")
log("=" * 70)

nielsen_brands_sql = """
SELECT DISTINCT
    CASE
        WHEN TRIM(UPPER(INP_56985)) IN ('ACTIVIA') THEN 'ACTIVIA'
        WHEN TRIM(UPPER(INP_56985)) IN ('BENEGASTRO','BENEG') THEN 'BENEGASTRO'
        WHEN TRIM(UPPER(INP_56985)) IN ('DANETTE') THEN 'DANETTE'
        WHEN TRIM(UPPER(INP_56985)) IN ('DANMIX','DAN MIX','DANMMIX') THEN 'DANMIX'
        WHEN TRIM(UPPER(INP_56985)) IN ('DANONE','DANONE YOGHURT','DANONE CREME','DANONE FREE',
                                         'DANONE GRIEGO','DANONE FS','DAIRY','DAIRY DANONE MEXICO','DANAO') THEN 'DANONE'
        WHEN TRIM(UPPER(INP_56985)) IN ('DANONINO','DANONINOLIQUIDO') THEN 'DANONINO'
        WHEN TRIM(UPPER(INP_56985)) IN ('DANUP','DAN UP') THEN 'DANUP'
        WHEN TRIM(UPPER(INP_56985)) IN ('DANY','DANY DANETTE') THEN 'DANY'
        WHEN TRIM(UPPER(INP_56985)) IN ('HERSHEYS','HERSHEY''S','DANONE HERSHEYS') THEN 'HERSHEYS'
        WHEN TRIM(UPPER(INP_56985)) IN ('LICUAMIX') THEN 'LICUAMIX'
        WHEN TRIM(UPPER(INP_56985)) IN ('OIKOS','OIKOS UHT') THEN 'OIKOS'
        WHEN TRIM(UPPER(INP_56985)) IN ('SILK','SILK ORIG 946ML','SILKCHOCO190ML') THEN 'SILK'
        WHEN TRIM(UPPER(INP_56985)) IN ('SO DELICIOUS') THEN 'SO DELICIOUS'
        WHEN TRIM(UPPER(INP_56985)) IN ('VITALINEA') THEN 'VITALINEA'
        WHEN TRIM(UPPER(INP_56985)) IN ('YOPRO','YO PRO') THEN 'YOPRO'
        WHEN TRIM(UPPER(INP_56985)) IN ('OCEAN SPRAY','OCEAN') THEN 'OCEAN SPRAY'
        WHEN TRIM(UPPER(INP_56985)) IN ('LALA','GPO INDUSTRIAL LALA') THEN 'LALA'
        ELSE TRIM(UPPER(INP_56985))
    END AS MARCA_STD
FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_PROD_DIM
WHERE INP_56985 IS NOT NULL
"""

# EDP Nielsen uses PRD_MEX; SELL_OUT brand list already captured (si_brands not reusable — different source)
sell_out_brands_for_nlsn_sql = """
SELECT DISTINCT
    CASE
        WHEN TRIM(UPPER(prod.BRAND)) IN ('ACTIVIA') THEN 'ACTIVIA'
        WHEN TRIM(UPPER(prod.BRAND)) IN ('BENEGASTRO','BENEG') THEN 'BENEGASTRO'
        WHEN TRIM(UPPER(prod.BRAND)) IN ('DANETTE') THEN 'DANETTE'
        WHEN TRIM(UPPER(prod.BRAND)) IN ('DANMIX','DAN MIX','DANMMIX') THEN 'DANMIX'
        WHEN TRIM(UPPER(prod.BRAND)) IN ('DANONE','DANONE YOGHURT','DANONE CREME','DANONE FREE',
                                          'DANONE GRIEGO','DANONE FS','DAIRY','Danone','DAIRY DANONE MEXICO','DANAO') THEN 'DANONE'
        WHEN TRIM(UPPER(prod.BRAND)) IN ('DANONINO','DANONINOLIQUIDO') THEN 'DANONINO'
        WHEN TRIM(UPPER(prod.BRAND)) IN ('DANUP','DAN UP','DAN''UP') THEN 'DANUP'
        WHEN TRIM(UPPER(prod.BRAND)) IN ('DANY','DANY DANETTE') THEN 'DANY'
        WHEN TRIM(UPPER(prod.BRAND)) IN ('HERSHEYS','HERSHEY''S','DANONE HERSHEYS') THEN 'HERSHEYS'
        WHEN TRIM(UPPER(prod.BRAND)) IN ('OIKOS','OIKOS UHT') THEN 'OIKOS'
        WHEN TRIM(UPPER(prod.BRAND)) IN ('SILK','SILK ORIG 946ML','SILKCHOCO190ML','Silk') THEN 'SILK'
        WHEN TRIM(UPPER(prod.BRAND)) IN ('SO DELICIOUS') THEN 'SO DELICIOUS'
        WHEN TRIM(UPPER(prod.BRAND)) IN ('VITALINEA') THEN 'VITALINEA'
        WHEN TRIM(UPPER(prod.BRAND)) IN ('YOPRO','YO PRO') THEN 'YOPRO'
        WHEN TRIM(UPPER(prod.BRAND)) IN ('OCEAN SPRAY','OCEAN') THEN 'OCEAN SPRAY'
        ELSE TRIM(UPPER(prod.BRAND))
    END AS MARCA_STD
FROM PRD_MDP.MDP_DSP.VW_FACT_SELL_OUT f
INNER JOIN PRD_MDP.MDP_DWH.V_D_PERIOD per ON f.PER_ID = per.PER_ID
INNER JOIN PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM prod
    ON TO_VARCHAR(f.UPC) = TO_VARCHAR(prod.INT_ID)
    AND f.CBU_ID = prod.CBU_ID
"""

try:
    df_so_nlsn, _ = run_sf_query("PRD_MDP", sell_out_brands_for_nlsn_sql, "V13E: SELL_OUT brands for Nielsen comparison")
    df_nlsn, _    = run_sf_query("PRD_MEX", nielsen_brands_sql,           "V13E: EDP Nielsen distinct MARCA_STD")

    # Cross-DB comparison in Python
    so_brands_set   = {r["MARCA_STD"] for r in df_so_nlsn.collect() if r["MARCA_STD"]}
    nlsn_brands_set = {r["MARCA_STD"] for r in df_nlsn.collect()    if r["MARCA_STD"]}

    matched_e   = so_brands_set & nlsn_brands_set
    unmatched_e = so_brands_set - nlsn_brands_set
    match_pct_e = len(matched_e) * 100.0 / max(len(so_brands_set), 1)

    log(f"  V13E — SO brands: {len(so_brands_set)}  "
        f"Nielsen EDP brands: {len(nlsn_brands_set)}  "
        f"Matched: {len(matched_e)}  "
        f"Unmatched: {len(unmatched_e)}  "
        f"Match%: {match_pct_e:.1f}%")

    if unmatched_e:
        log(f"  SELL_OUT brands not in EDP Nielsen (expected — waters/EDP gap): {sorted(unmatched_e)}")

    # Nielsen lag check
    nielsen_lag_sql = """
    SELECT
        MAX(DATE_TRUNC('MONTH', TO_DATE(TO_VARCHAR(PER_DATE_BEG),'YYYYMMDD'))) AS MAX_NIELSEN_MONTH
    FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_PER_DIM
    """
    df_lag, _ = run_sf_query("PRD_MEX", nielsen_lag_sql, "V13E: Nielsen max period")
    nielsen_max = df_lag.collect()[0]["MAX_NIELSEN_MONTH"]
    log(f"  Nielsen EDP max month : {nielsen_max}")
    log(f"  SELL_OUT max month    : {SO_MAX_MONTH}")

    if nielsen_max and SO_MAX_MONTH:
        from datetime import date
        so_dt  = SO_MAX_MONTH  if hasattr(SO_MAX_MONTH,  "toordinal") else date.fromisoformat(str(SO_MAX_MONTH)[:10])
        nl_dt  = nielsen_max   if hasattr(nielsen_max,   "toordinal") else date.fromisoformat(str(nielsen_max)[:10])
        lag_days  = (so_dt - nl_dt).days
        lag_weeks = lag_days / 7.0
        log(f"  Nielsen lag: {lag_weeks:.1f} weeks behind SELL_OUT")
        if lag_weeks > 8:
            warn(f"V13E: Nielsen lag = {lag_weeks:.1f} weeks (threshold: 8 weeks). "
                 f"Nielsen KPIs will populate NULL for months beyond {nl_dt}.")
    else:
        warn("V13E: Could not compute Nielsen lag — max period date unavailable in PER_DIM.")

    if match_pct_e < 60.0:
        warn(f"V13E: SELL_OUT × EDP Nielsen match = {match_pct_e:.1f}% (threshold: 60%). "
             f"Waters/EDP split expected — review if share KPIs are needed for unmatched brands.")
    else:
        log(f"  ✅ V13E PASS — match {match_pct_e:.1f}% >= 60%")

except Exception as e:
    blocker(f"V13E query failed: {e}")

abort_if_blockers()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Summary

# COMMAND ----------

log("")
log("=" * 70)
log("V13 VALIDATION SUMMARY")
log("=" * 70)
log(f"Run ID     : {RUN_ID}")
log(f"Completed  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
log("")
log(f"Hard Blockers : {len(BLOCKERS)}")
for b in BLOCKERS:
    log(f"  {b}")
log("")
log(f"Warnings      : {len(WARNINGS)}")
for w in WARNINGS:
    log(f"  {w}")
log("")
if not BLOCKERS:
    log("✅ ALL HARD BLOCKERS PASSED — Stage 2 is cleared to proceed.")
    log("   Review warnings above before starting Stage 2.")
else:
    log("❌ STAGE 1 FAILED — Fix all blockers before Stage 2.")
log("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write log file

# COMMAND ----------

log_candidates = get_log_path()
written        = False
log_text       = "\n".join(LOG_LINES)

for path in log_candidates:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(log_text)
        print(f"Log written: {path}")
        written = True
        break
    except Exception as e:
        print(f"Could not write to {path}: {e}")

if not written:
    print("WARNING: Could not write log to any candidate path.")
    print(log_text[:500])
