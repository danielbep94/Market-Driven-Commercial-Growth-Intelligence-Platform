# Databricks notebook source
# MAGIC %md
# MAGIC # V13 — Cross-Source Join Validation (Phase D — Stage 1)
# MAGIC
# MAGIC **Grain:** `MARCA_STD + MONTH` (brand-month, no CADENA/CANAL)
# MAGIC
# MAGIC **Checks:**
# MAGIC - V13A: SELL_OUT × MKT_ON      (warn if < 70% Danone brand overlap)
# MAGIC - V13B: SELL_OUT × MKT_OFF     (warn if < 70%, Danone only)
# MAGIC - V13C: SELL_OUT × WASTE       (warn if < 80%)
# MAGIC - V13D: SELL_IN × IBP          (warn if < 80%)
# MAGIC - V13E: SELL_OUT × EDP Nielsen (warn if < 60%) + Nielsen lag check
# MAGIC
# MAGIC **Pattern:** One SELECT per source → Python set operations for overlap.
# MAGIC No cross-source SQL joins. No f-string SQL injection. No DDL commands.
# MAGIC
# MAGIC **Log output:** `notebooks/validation_results_phase_d.txt`

# COMMAND ----------

import os
import sys
import io
import uuid
import importlib.util
from datetime import datetime

# ═══════════════════════════════════════════════════════════════════════════════
# Credential resolution
# Priority: (1) configs/snowflake_creds.py  (2) Databricks KV  (3) env var
# Cross-DB: PRD_MEX + PRD_MDP cannot share one Snowflake session (ISSUE-003).
# ═══════════════════════════════════════════════════════════════════════════════

SF_URL       = "danonenam.east-us-2.azure.snowflakecomputing.com"
KV_SCOPE_MEX = "DAN-AM-P-KVT800-R-MEX-DB"
KV_SCOPE_MDP = "DAN-AM-P-KVT800-R-MDP-DB"

_creds = None

def _find_file_in_configs(filename):
    """
    Locate a file inside configs/ without relying on __file__.
    Databricks notebooks have no __file__, so we probe well-known paths.
    Resolution order:
      1. __file__-relative (local / pytest)
      2. CWD/configs/<filename>
      3. CWD/../configs/<filename>  (when CWD == notebooks/)
      4. git rev-parse --show-toplevel / configs/<filename>
    """
    candidates = []
    try:
        candidates.append(
            os.path.normpath(
                os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "configs", filename)
            )
        )
    except Exception:
        pass
    try:
        cwd = os.getcwd()
        candidates.append(os.path.join(cwd, "configs", filename))
        candidates.append(os.path.normpath(os.path.join(cwd, "..", "configs", filename)))
        if os.path.basename(cwd).lower() == "notebooks":
            candidates.append(os.path.join(os.path.dirname(cwd), "configs", filename))
    except Exception:
        pass
    try:
        import subprocess
        repo_root = subprocess.check_output(
            ["git", "rev-parse", "--show-toplevel"], stderr=subprocess.DEVNULL
        ).decode().strip()
        candidates.append(os.path.join(repo_root, "configs", filename))
    except Exception:
        pass
    for p in candidates:
        if os.path.exists(p):
            return p
    raise FileNotFoundError(
        f"Cannot find configs/{filename}. Tried:\n" + "\n".join(f"  {c}" for c in candidates)
    )

# ─── Load credentials ─────────────────────────────────────────────────────────
for _cp in [_find_file_in_configs("snowflake_creds.py")] if True else []:
    try:
        _spec = importlib.util.spec_from_file_location("snowflake_creds", _cp)
        _m    = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_m)
        _creds = _m
        print(f"  [credentials] Loaded from {_cp}")
    except FileNotFoundError:
        print("  [credentials] snowflake_creds.py not found via filesystem — will try KV")
    except Exception as _e:
        print(f"  [credentials] Could not load creds: {_e}")


def _secret(local_val, scope, key, env_var=None):
    """Resolve credential: (1) local creds file → (2) Databricks KV → (3) env var."""
    if local_val is not None:
        return local_val
    try:
        return dbutils.secrets.get(scope=scope, key=key)   # noqa: F821
    except Exception:
        pass
    val = os.getenv(env_var) if env_var else None
    if val:
        return val
    raise RuntimeError(
        f"Cannot resolve '{key}'. "
        f"Ensure configs/snowflake_creds.py exists on the Databricks workspace, "
        f"OR that KV scope '{scope}' contains key '{key}'."
    )


# ─── Connection profiles ──────────────────────────────────────────────────────
PRD_MEX_PROFILE = {
    "sfURL":       SF_URL,
    "sfUser":      _secret(getattr(_creds, "SF_MEX_USER",     None), KV_SCOPE_MEX, "snowflake-mex-user",     "SF_MEX_USER"),
    "sfPassword":  _secret(getattr(_creds, "SF_MEX_PASSWORD", None), KV_SCOPE_MEX, "snowflake-mex-password", "SF_MEX_PASSWORD"),
    "sfWarehouse": getattr(_creds, "SF_MEX_WH",   "PRD_MEX_ANL_WH"),
    "sfRole":      getattr(_creds, "SF_MEX_ROLE",  "PRD_MEX_READER"),
}

PRD_MDP_PROFILE = {
    "sfURL":       SF_URL,
    "sfUser":      _secret(getattr(_creds, "SF_MDP_USER",     None), KV_SCOPE_MDP, "snowflake-user",     "SF_MDP_USER"),
    "sfPassword":  _secret(getattr(_creds, "SF_MDP_PASSWORD", None), KV_SCOPE_MDP, "snowflake-password", "SF_MDP_PASSWORD"),
    "sfWarehouse": getattr(_creds, "SF_MDP_WH",   "PRD_MDP_ANL_WH"),
    "sfRole":      getattr(_creds, "SF_MDP_ROLE",  "PRD_MDP"),
}

# ─── Run metadata ─────────────────────────────────────────────────────────────
RUN_ID = str(uuid.uuid4())
RUN_TS = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ─── Logging ──────────────────────────────────────────────────────────────────
LOG_LINES = []
BLOCKERS  = []
WARNINGS  = []


def log(msg=""):
    ts   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    LOG_LINES.append(line)


def log_df(df, label, n=20):
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


def blocker(msg):
    full = f"[BLOCKER] {msg}"
    log(full)
    BLOCKERS.append(full)


def warn(msg):
    full = f"[WARNING] {msg}"
    log(full)
    WARNINGS.append(full)


def abort_if_blockers():
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
    """Run a SELECT query against Snowflake via the Spark connector."""
    profile = PRD_MDP_PROFILE if database == "PRD_MDP" else PRD_MEX_PROFILE
    opts    = {**profile, "sfDatabase": database}
    log(f"  Running: {label}  [db={database}]")
    df = (
        spark.read                                             # noqa: F821
        .format("net.snowflake.spark.snowflake")
        .options(**opts)
        .option("sfDatabase", database)
        .option("query", query)
        .load()
    )
    rc = df.count()
    log(f"  → {rc} rows returned")
    return df, rc


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

## Header

log("=" * 70)
log("V13 — Phase D Stage 1: Cross-Source Join Validation")
log("=" * 70)
log(f"Run ID        : {RUN_ID}")
log(f"Run timestamp : {RUN_TS}")
log(f"Mart target   : PRD_MDP.MDP_STG.MART_MARKET_GROWTH_INTELLIGENCE_MONTHLY")
log(f"Grain         : MARCA_STD + MONTH (brand-month)")
log(f"Pattern       : one SELECT per source → Python set operations (no cross-source SQL joins)")
log("")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Danone brand list — loaded from brand_crosswalk.yaml at runtime

# COMMAND ----------

import yaml

_crosswalk_path = _find_file_in_configs("brand_crosswalk.yaml")
log(f"  brand_crosswalk.yaml → {_crosswalk_path}")
with open(_crosswalk_path, "r", encoding="utf-8") as _f:
    _crosswalk = yaml.safe_load(_f)

DANONE_BRANDS     = set(_crosswalk.get("danone_brands", {}).keys())
DANONE_BRANDS_SET = DANONE_BRANDS  # alias for readability

log(f"  Danone brands loaded: {len(DANONE_BRANDS)}")
log(f"  → {sorted(DANONE_BRANDS)}")
log("")

# COMMAND ----------

# MAGIC %md
# MAGIC ## PRE-CHECK A — PRD_MDP connection + role

# COMMAND ----------

log("=" * 70)
log("PRE-CHECK A — PRD_MDP connection health")
log("=" * 70)

conn_sql = """
SELECT
    CURRENT_DATABASE()  AS DB,
    CURRENT_ROLE()      AS ROLE,
    CURRENT_WAREHOUSE() AS WH
"""
try:
    df_conn, _ = run_sf_query("PRD_MDP", conn_sql, "PRE-CHECK: PRD_MDP connection")
    row = df_conn.collect()[0]
    log(f"  DB={row['DB']}  ROLE={row['ROLE']}  WH={row['WH']}")
    if str(row["ROLE"]) != "PRD_MDP":
        blocker(f"Active role is '{row['ROLE']}' — expected 'PRD_MDP'. Fix sfRole in snowflake_creds.py.")
    else:
        log("  ✅ PRD_MDP role confirmed")
except Exception as e:
    blocker(f"PRD_MDP connection failed: {e}")

abort_if_blockers()

# COMMAND ----------

# MAGIC %md
# MAGIC ## PRE-CHECK B — MDP_STG schema visibility

# COMMAND ----------

log("=" * 70)
log("PRE-CHECK B — MDP_STG schema visibility via INFORMATION_SCHEMA")
log("=" * 70)

schema_sql = """
SELECT TABLE_SCHEMA, COUNT(*) AS TABLE_COUNT
FROM PRD_MDP.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'MDP_STG'
GROUP BY TABLE_SCHEMA
"""
try:
    df_schema, _ = run_sf_query("PRD_MDP", schema_sql, "PRE-CHECK: MDP_STG in INFORMATION_SCHEMA")
    rows = df_schema.collect()
    if rows:
        log(f"  ✅ MDP_STG schema visible — {rows[0]['TABLE_COUNT']} table(s) found")
    else:
        log("  ⚠️  MDP_STG schema returned 0 rows (empty schema or first run — acceptable)")
        warn("PRE-CHECK: MDP_STG schema empty. Confirm PRD_MDP role has USAGE on PRD_MDP.MDP_STG.")
except Exception as e:
    blocker(f"MDP_STG schema check failed: {e}. DBA must grant USAGE + CREATE TABLE + INSERT on PRD_MDP.MDP_STG to role PRD_MDP.")

abort_if_blockers()

# COMMAND ----------

# MAGIC %md
# MAGIC ## V13-PRE — SELL_OUT MARCA_STD zero-tolerance NULL check

# COMMAND ----------

log("=" * 70)
log("V13-PRE — SELL_OUT MARCA_STD zero-tolerance NULL check")
log("Rule: NULL rate must be exactly 0%. Any NULL = hard BLOCKER.")
log("=" * 70)

sell_out_null_sql = """
SELECT
    COUNT(*)                                                AS TOTAL_ROWS,
    SUM(CASE WHEN MARCA_STD IS NULL THEN 1 ELSE 0 END)     AS NULL_MARCA_STD,
    SUM(CASE WHEN MARCA_STD IS NULL THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(*), 0)                               AS NULL_PCT,
    COUNT(DISTINCT MARCA_STD)                               AS DISTINCT_BRANDS,
    COUNT(DISTINCT DATE_TRUNC('MONTH', FECHA))              AS DISTINCT_MONTHS,
    MIN(DATE_TRUNC('MONTH', FECHA))                         AS MIN_MONTH,
    MAX(DATE_TRUNC('MONTH', FECHA))                         AS MAX_MONTH
FROM (
    SELECT
        per."DAY" AS FECHA,
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
    FROM PRD_MDP.MDP_DSP.VW_FACT_SELL_OUT f
    INNER JOIN PRD_MDP.MDP_DWH.V_D_PERIOD per
        ON f.PER_ID = per.PER_ID
    INNER JOIN PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM prod
        ON TO_VARCHAR(f.UPC) = TO_VARCHAR(prod.INT_ID)
        AND f.CBU_ID = prod.CBU_ID
) t
"""

SO_MAX_MONTH       = None
SO_DISTINCT_BRANDS = 0
SO_DISTINCT_MONTHS = 0

try:
    df_pre, _ = run_sf_query("PRD_MDP", sell_out_null_sql, "V13-PRE: SELL_OUT MARCA_STD null check")
    log_df(df_pre, "SELL_OUT summary", n=5)
    r = df_pre.collect()[0]
    null_cnt   = int(r["NULL_MARCA_STD"] or 0)
    null_pct   = float(r["NULL_PCT"]     or 0.0)
    total_rows = int(r["TOTAL_ROWS"]     or 0)
    SO_DISTINCT_BRANDS = int(r["DISTINCT_BRANDS"] or 0)
    SO_DISTINCT_MONTHS = int(r["DISTINCT_MONTHS"] or 0)
    SO_MAX_MONTH       = r["MAX_MONTH"]

    log(f"  TOTAL_ROWS={total_rows:,}  NULL_MARCA_STD={null_cnt}  NULL_PCT={null_pct:.4f}%")
    log(f"  DISTINCT_BRANDS={SO_DISTINCT_BRANDS}  DISTINCT_MONTHS={SO_DISTINCT_MONTHS}")
    log(f"  MIN_MONTH={r['MIN_MONTH']}  MAX_MONTH={SO_MAX_MONTH}")

    if null_cnt > 0:
        blocker(f"SELL_OUT MARCA_STD NULL count = {null_cnt} ({null_pct:.4f}%). "
                f"Zero tolerance — investigate unmapped BRAND values.")
    else:
        log("  ✅ V13-PRE PASS — SELL_OUT MARCA_STD: 0 NULLs")

    if total_rows > 0 and r["MIN_MONTH"] is None:
        blocker("FECHA DATE_TRUNC returned NULL for all rows — date column may be uncastable.")
    else:
        log("  ✅ DATE_TRUNC OK")

except Exception as e:
    blocker(f"V13-PRE query failed: {e}")

abort_if_blockers()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared — SELL_OUT distinct MARCA_STD (queried once, reused by V13A/B/C)

# COMMAND ----------

log("=" * 70)
log("SHARED — SELL_OUT distinct MARCA_STD")
log("=" * 70)

so_brands_sql = """
SELECT DISTINCT
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
FROM PRD_MDP.MDP_DSP.VW_FACT_SELL_OUT f
INNER JOIN PRD_MDP.MDP_DWH.V_D_PERIOD per
    ON f.PER_ID = per.PER_ID
INNER JOIN PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM prod
    ON TO_VARCHAR(f.UPC) = TO_VARCHAR(prod.INT_ID)
    AND f.CBU_ID = prod.CBU_ID
WHERE prod.BRAND IS NOT NULL
"""

SO_ALL_BRANDS    = set()
SO_DANONE_BRANDS = set()

try:
    df_so, _ = run_sf_query("PRD_MDP", so_brands_sql, "SHARED: SELL_OUT distinct MARCA_STD")
    SO_ALL_BRANDS    = {r["MARCA_STD"] for r in df_so.collect() if r["MARCA_STD"]}
    SO_DANONE_BRANDS = SO_ALL_BRANDS & DANONE_BRANDS_SET
    log(f"  SELL_OUT total distinct brands: {len(SO_ALL_BRANDS)}")
    log(f"  SELL_OUT Danone brands:         {len(SO_DANONE_BRANDS)}")
    log(f"  → {sorted(SO_DANONE_BRANDS)}")
except Exception as e:
    blocker(f"SELL_OUT shared brand query failed: {e}")

abort_if_blockers()

# COMMAND ----------

# MAGIC %md
# MAGIC ## V13A — SELL_OUT × MKT_ON (warn if < 70%)

# COMMAND ----------

log("=" * 70)
log("V13A — SELL_OUT × MKT_ON  |  threshold: WARN if < 70%")
log("=" * 70)

mkt_on_sql = """
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
        WHEN TRIM(UPPER(MARCA)) IN ('DANISSIMO') THEN 'DANISSIMO'
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
WHERE MARCA IS NOT NULL
  AND TRIM(UPPER(MARCA)) NOT IN (
      'MULTIBRAND','MULTIBRAND DAIRY','MULTIBRAND DANONE','MULTIBRAND INDULGENCE',
      'MULTIBRAND INNOS','MULTIBRAND KIDS','MULTIBRAND WATERS','MULTIMARCA',
      'INSTITUTO DANONE','DNP','DANONE FS','_MERCHANDISE','_UNKNOWN','_MULTIBRAND'
  )
"""

try:
    df_a, _ = run_sf_query("PRD_MDP", mkt_on_sql, "V13A: MKT_ON distinct MARCA_STD")
    mkt_on_brands = {r["MARCA_STD"] for r in df_a.collect() if r["MARCA_STD"]}

    matched_a   = SO_DANONE_BRANDS & mkt_on_brands
    unmatched_a = SO_DANONE_BRANDS - mkt_on_brands
    pct_a       = len(matched_a) * 100.0 / max(len(SO_DANONE_BRANDS), 1)

    log(f"  MKT_ON brands: {len(mkt_on_brands)}  |  SO Danone: {len(SO_DANONE_BRANDS)}  "
        f"|  Matched: {len(matched_a)}  |  Match%: {pct_a:.1f}%")
    if unmatched_a:
        log(f"  Unmatched: {sorted(unmatched_a)}")
    if pct_a < 70.0:
        warn(f"V13A: {pct_a:.1f}% match (< 70%). Unmatched Danone brands: {sorted(unmatched_a)}")
    else:
        log(f"  ✅ V13A PASS — {pct_a:.1f}% >= 70%")
except Exception as e:
    blocker(f"V13A query failed: {e}")

abort_if_blockers()

# COMMAND ----------

# MAGIC %md
# MAGIC ## V13B — SELL_OUT × MKT_OFF (Danone only, warn if < 70%)

# COMMAND ----------

log("=" * 70)
log("V13B — SELL_OUT × MKT_OFF (Danone only)  |  threshold: WARN if < 70%")
log("=" * 70)

mkt_off_sql = """
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
        WHEN TRIM(UPPER(MARCA)) IN ('DANISSIMO') THEN 'DANISSIMO'
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
WHERE MARCA IS NOT NULL
  AND INVERSION_REAL IS NOT NULL
  AND TRIM(UPPER(MARCA)) NOT IN (
      'COCA COLA','COCA-COLA','COCACOLA','THE COCA COLA EXPORT','COCA COLA FEMSA',
      'PEPSI','PEPSI COLA MEXICANA','PEPSICOLA MEXICANA',
      'LALA','GPO INDUSTRIAL LALA','LALA_BIO4','LALA_CHIQUITIN','LALA_CREMA',
      'LALA_GRIEGO','LALA_LECHE','LALA_LECHE_100','LALA_MAESTROS','LALA_NATURALES',
      'LALA_PLENIA','LALA_PROBIOC','LALA_QUESOS','LALA_YOGURT','LALA_YOGURT_100','LALA_YOMI',
      'ALPURA','ALPURA_GRIEGO','ALPURA_LECHES','ALPURA_TOYS','ALPURA_YOGURT',
      'ALPURA_YOGURT_COLAGENO','ALPURA_YOGURT_DESLACTOSADO',
      'YOPLAIT','YOPLAIT_DOBLECERO','YOPLAIT_GRIEGO','YOPLAIT_GRIE_GO','YOPLAIT_KIDS','YOPLAIT_SKYR',
      'JUMEX','JUMEXITO','JUMEX_AMI','JUMEX_BIDA','JUMEX_CERO','JUMEX_FRESCO','JUMEX_FRESH',
      'JUMEX_FRUTZZO','JUMEX_HYDROLIT','JUMEX_MIA','JUMEX_SPORT','JUMEX_UNICO','JUMEX_XODA',
      'JUMEX_XOT','JUGOS DEL VALLE',
      'SANTA CLARA','SANTA_CLARA','SOC COOP TRAB PASCUA',
      'CIEL','CIEL_EXPRIM','CIEL_MINERAL',
      'PENAFIEL','PEÑAFIEL','SEVEN UP','SEVENUP','SEVEN UP CHI',
      'MULTIBRAND','MULTIBRAND DAIRY','MULTIBRAND DANONE','MULTIBRAND INDULGENCE',
      'MULTIBRAND INNOS','MULTIBRAND KIDS','MULTIBRAND WATERS','MULTIMARCA',
      'INSTITUTO DANONE','DNP','_MERCHANDISE','_UNKNOWN','_MULTIBRAND'
  )
"""

try:
    df_b, _ = run_sf_query("PRD_MDP", mkt_off_sql, "V13B: MKT_OFF distinct MARCA_STD (Danone filter)")
    mkt_off_all    = {r["MARCA_STD"] for r in df_b.collect() if r["MARCA_STD"]}
    mkt_off_danone = mkt_off_all & DANONE_BRANDS_SET

    matched_b   = SO_DANONE_BRANDS & mkt_off_danone
    unmatched_b = SO_DANONE_BRANDS - mkt_off_danone
    pct_b       = len(matched_b) * 100.0 / max(len(SO_DANONE_BRANDS), 1)

    log(f"  MKT_OFF Danone brands: {len(mkt_off_danone)}  |  SO Danone: {len(SO_DANONE_BRANDS)}  "
        f"|  Matched: {len(matched_b)}  |  Match%: {pct_b:.1f}%")
    if unmatched_b:
        log(f"  Unmatched (expected for brands with no OFF spend): {sorted(unmatched_b)}")
    if pct_b < 70.0:
        warn(f"V13B: {pct_b:.1f}% match (< 70%). Unmatched Danone brands: {sorted(unmatched_b)}")
    else:
        log(f"  ✅ V13B PASS — {pct_b:.1f}% >= 70%")
except Exception as e:
    blocker(f"V13B query failed: {e}")

abort_if_blockers()

# COMMAND ----------

# MAGIC %md
# MAGIC ## V13C — SELL_OUT × WASTE (warn if < 80%)

# COMMAND ----------

log("=" * 70)
log("V13C — SELL_OUT × WASTE  |  threshold: WARN if < 80%")
log("=" * 70)

waste_sql = """
SELECT DISTINCT
    CASE
        WHEN TRIM(UPPER(MARCA)) IN ('ACTIVIA') THEN 'ACTIVIA'
        WHEN TRIM(UPPER(MARCA)) IN ('AGUAS FRESCAS','BFT AGUAS FRESCAS','BONAFONT_AGUASFRESCAS','BONAFONT AGUA FRESCAS') THEN 'AGUAS FRESCAS'
        WHEN TRIM(UPPER(MARCA)) IN ('BENEGASTRO','BENEG') THEN 'BENEGASTRO'
        WHEN TRIM(UPPER(MARCA)) IN ('BONAFONT','BONAFONT_NATURAL','AZUL BONAFONT') THEN 'BONAFONT'
        WHEN TRIM(UPPER(MARCA)) IN ('BONAFONT KIDS','BONAFONT_KIDS','BONAFONT KIDS NATURAL') THEN 'BONAFONT KIDS'
        WHEN TRIM(UPPER(MARCA)) IN ('BONAFONT MINERAL','MINERALIZADA') THEN 'BONAFONT MINERAL'
        WHEN TRIM(UPPER(MARCA)) IN ('BONAFONT SER','SER PURA') THEN 'BONAFONT SER'
        WHEN TRIM(UPPER(MARCA)) IN ('TE BONAFONT','TÉ BONAFONT','BONAFONT TE','BONAFONT_TE','BONAFONT TÉ') THEN 'BONAFONT TE'
        WHEN TRIM(UPPER(MARCA)) IN ('DANETTE') THEN 'DANETTE'
        WHEN TRIM(UPPER(MARCA)) IN ('DANISSIMO') THEN 'DANISSIMO'
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
  AND TRIM(UPPER(MARCA)) NOT IN ('0','MULTI','DAIRY','_UNKNOWN')
"""

try:
    df_c, _ = run_sf_query("PRD_MDP", waste_sql, "V13C: WASTE distinct MARCA_STD")
    waste_brands = {r["MARCA_STD"] for r in df_c.collect() if r["MARCA_STD"]}

    matched_c   = SO_ALL_BRANDS & waste_brands
    unmatched_c = SO_ALL_BRANDS - waste_brands
    pct_c       = len(matched_c) * 100.0 / max(len(SO_ALL_BRANDS), 1)

    log(f"  WASTE brands: {len(waste_brands)}  |  SO brands: {len(SO_ALL_BRANDS)}  "
        f"|  Matched: {len(matched_c)}  |  Match%: {pct_c:.1f}%")
    if unmatched_c:
        log(f"  SO brands not in WASTE: {sorted(unmatched_c)}")
    if pct_c < 80.0:
        warn(f"V13C: {pct_c:.1f}% match (< 80%). Brands with no waste record: {sorted(unmatched_c)}")
    else:
        log(f"  ✅ V13C PASS — {pct_c:.1f}% >= 80%")
except Exception as e:
    blocker(f"V13C query failed: {e}")

abort_if_blockers()

# COMMAND ----------

# MAGIC %md
# MAGIC ## V13D — SELL_IN × IBP (warn if < 80%)
# MAGIC Note: cross-DB — SELL_IN (PRD_MEX) vs IBP (PRD_MDP). Queried separately, joined in Python.

# COMMAND ----------

log("=" * 70)
log("V13D — SELL_IN × IBP  |  threshold: WARN if < 80%")
log("Note: cross-DB — separate queries, Python set join (ISSUE-003)")
log("=" * 70)

sell_in_sql = """
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
  AND TRIM(UPPER(PRO.LV2_UMB_BRD_DSC)) NOT IN (
      '_MERCHANDISE','_UNKNOWN','LONCHERA','PALAS','TABLAS','TAZAS','VASOS','TOALLAS',
      'TUPPER','UTENSILIOS','CERAMICA','VAJILLA','LAPICERA','COSMETIQUERA',
      'PLAYERA MUNDIAL','PORTAVASOS','REFRACTARIOS','BOTANERO','DISPENSERS',
      'BEBEDERO','BOMBA','JARRA','TERMO','ALCANCIA','METALICO','ESPECIERO','POUCH','BOWLS',
      'CODISTRIBUCION','OTHERS','PRIVADA','PROMO','KINDER'
  )
"""

ibp_sql = """
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
    df_si, _ = run_sf_query("PRD_MEX", sell_in_sql, "V13D: SELL_IN distinct MARCA_STD")
    df_ib, _ = run_sf_query("PRD_MDP", ibp_sql,     "V13D: IBP distinct MARCA_STD")

    si_brands  = {r["MARCA_STD"] for r in df_si.collect() if r["MARCA_STD"]}
    ibp_brands = {r["MARCA_STD"] for r in df_ib.collect() if r["MARCA_STD"]}

    matched_d   = si_brands & ibp_brands
    unmatched_d = si_brands - ibp_brands
    pct_d       = len(matched_d) * 100.0 / max(len(si_brands), 1)

    log(f"  SELL_IN brands: {len(si_brands)}  |  IBP brands: {len(ibp_brands)}  "
        f"|  Matched: {len(matched_d)}  |  Match%: {pct_d:.1f}%")
    if unmatched_d:
        log(f"  SELL_IN brands not in IBP: {sorted(unmatched_d)}")
    if pct_d < 80.0:
        warn(f"V13D: {pct_d:.1f}% match (< 80%). Unmatched: {sorted(unmatched_d)}")
    else:
        log(f"  ✅ V13D PASS — {pct_d:.1f}% >= 80%")
except Exception as e:
    blocker(f"V13D query failed: {e}")

abort_if_blockers()

# COMMAND ----------

# MAGIC %md
# MAGIC ## V13E — SELL_OUT × EDP Nielsen (warn if < 60%) + Nielsen lag

# COMMAND ----------

log("=" * 70)
log("V13E — SELL_OUT × EDP Nielsen  |  threshold: WARN if < 60%")
log("=" * 70)

nielsen_sql = """
SELECT DISTINCT
    CASE
        WHEN TRIM(UPPER(INP_56985)) IN ('ACTIVIA') THEN 'ACTIVIA'
        WHEN TRIM(UPPER(INP_56985)) IN ('BENEGASTRO','BENEG') THEN 'BENEGASTRO'
        WHEN TRIM(UPPER(INP_56985)) IN ('DANETTE') THEN 'DANETTE'
        WHEN TRIM(UPPER(INP_56985)) IN ('DANMIX','DAN MIX','DANMMIX') THEN 'DANMIX'
        WHEN TRIM(UPPER(INP_56985)) IN ('DANONE','DANONE YOGHURT','DANONE CREME','DANONE FREE','DANONE GRIEGO','DANONE FS','DAIRY','DAIRY DANONE MEXICO','DANAO') THEN 'DANONE'
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

nielsen_lag_sql = """
SELECT MAX(DATE_TRUNC('MONTH', TO_DATE("period_ending_datetime"))) AS MAX_NIELSEN_MONTH
FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_PER_DIM
"""

try:
    df_nl, _  = run_sf_query("PRD_MEX", nielsen_sql,     "V13E: EDP Nielsen distinct MARCA_STD")
    df_lag, _ = run_sf_query("PRD_MEX", nielsen_lag_sql, "V13E: Nielsen max period")

    nielsen_brands = {r["MARCA_STD"] for r in df_nl.collect()  if r["MARCA_STD"]}
    nielsen_max    = df_lag.collect()[0]["MAX_NIELSEN_MONTH"]

    matched_e   = SO_ALL_BRANDS & nielsen_brands
    unmatched_e = SO_ALL_BRANDS - nielsen_brands
    pct_e       = len(matched_e) * 100.0 / max(len(SO_ALL_BRANDS), 1)

    log(f"  Nielsen EDP brands: {len(nielsen_brands)}  |  SO brands: {len(SO_ALL_BRANDS)}  "
        f"|  Matched: {len(matched_e)}  |  Match%: {pct_e:.1f}%")
    if unmatched_e:
        log(f"  SO brands not in EDP Nielsen (expected — waters/IBP split): {sorted(unmatched_e)}")

    # Nielsen lag
    log(f"  Nielsen EDP max month : {nielsen_max}")
    log(f"  SELL_OUT max month    : {SO_MAX_MONTH}")
    if nielsen_max and SO_MAX_MONTH:
        from datetime import date as _date
        so_dt  = SO_MAX_MONTH  if hasattr(SO_MAX_MONTH,  "toordinal") else _date.fromisoformat(str(SO_MAX_MONTH)[:10])
        nl_dt  = nielsen_max   if hasattr(nielsen_max,   "toordinal") else _date.fromisoformat(str(nielsen_max)[:10])
        lag_wk = (so_dt - nl_dt).days / 7.0
        log(f"  Nielsen lag: {lag_wk:.1f} weeks behind SELL_OUT")
        if lag_wk > 8:
            warn(f"V13E: Nielsen lag = {lag_wk:.1f} weeks (> 8 weeks threshold). "
                 f"Nielsen KPIs will be NULL for months beyond {nl_dt}.")
        else:
            log(f"  ✅ Nielsen lag {lag_wk:.1f} weeks — within 8-week threshold")
    else:
        warn("V13E: Could not compute Nielsen lag — max period date unavailable.")

    if pct_e < 60.0:
        warn(f"V13E: {pct_e:.1f}% match (< 60%). "
             f"Waters/EDP split expected — review if share KPIs needed for: {sorted(unmatched_e)}")
    else:
        log(f"  ✅ V13E PASS — {pct_e:.1f}% >= 60%")
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
log(f"Run ID    : {RUN_ID}")
log(f"Completed : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
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

log_text = "\n".join(LOG_LINES)
written  = False
for path in get_log_path():
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(log_text)
        print(f"Log written: {path}")
        written = True
        break
    except Exception as e:
        print(f"  Could not write to {path}: {e}")

if not written:
    print("WARNING: Could not write log to any candidate path.")
    print(log_text[:500])

# COMMAND ----------


