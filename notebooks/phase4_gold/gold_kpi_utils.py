# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 4 Gold — Shared KPI Utilities
# MAGIC **gold_kpi_utils.py** — run this notebook first via `%run ./gold_kpi_utils` in every Phase 4 notebook.
# MAGIC
# MAGIC Provides:
# MAGIC - Phase 4 configuration constants (GOLD_START_MONTH, GOLD_END_MONTH, RUN_MODE)
# MAGIC - Path resolution (local dev vs DBFS)
# MAGIC - Utility functions: safe_divide, assert_unique_keys, assert_no_join_fanout
# MAGIC - Logging helpers: log_gold, gold_blocker, gold_warn, gold_passed
# MAGIC - save_gold_df, register_gold_metric, validate_expected_columns
# MAGIC - write_audit_log
# MAGIC
# MAGIC Architectural rules (from implementation plan):
# MAGIC   - B14/B15 PRE-CONFIRMED 2026-06-27: zero Snowflake write operations.
# MAGIC   - All Phase 4 outputs: DBFS primary + logs/ audit only. No Snowflake writes.
# MAGIC   - Master grain: fecha_month x marca_std x canal_std x cadena_std
# MAGIC   - fecha_month = F.trunc(date_col, "MM") — always first-of-month

# COMMAND ----------

import os
import datetime
import yaml
import importlib.util

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType

# COMMAND ----------
# MAGIC %md ## 1. Phase 4 Configuration

# COMMAND ----------

# ── Run mode ─────────────────────────────────────────────────────────────────
# FULL   → production Gold build (Silver inputs must be complete-period)
# SAMPLE → dev/QA only — hard-blocked from producing Gold KPI outputs (B12)
RUN_MODE = "FULL"   # SET THIS BEFORE EVERY RUN

# ── Date range ────────────────────────────────────────────────────────────────
# Hard-block rows outside this range (B11)
GOLD_START_MONTH = "2022-01-01"   # Nielsen facts start 2022-10-09; SELL_IN has rows from 202602
GOLD_END_MONTH   = None   # None = auto-detect from Silver max(fecha_month)

# ── Output paths ─────────────────────────────────────────────────────────────
# Primary: DBFS (compute outputs)
# Audit:   logs/ repo directory (small files only — no large KPI CSVs in Git)
DBFS_GOLD_ROOT   = "dbfs:/mnt/mdp/mdm/phase4_gold/data"
LOCAL_GOLD_ROOT  = "/dbfs/mnt/mdp/mdm/phase4_gold/data"

# ── Silver CSV sentinel file — used to validate LOGS_DIR candidates ───────────
_SILVER_SENTINEL = "sell_in_std.csv"   # must exist in the correct logs/ dir

def _resolve_logs_dir():
    """Locate the project-root logs/ directory from any notebook CWD.

    Problem in Databricks:
      CWD = /Workspace/Users/<user>/notebooks/phase4_gold/
      The Databricks workspace root (/Workspace/Users/<user>/) also has a
      logs/ subdirectory that can contain unrelated CSV files, causing the
      generic CSV-presence guard to resolve to the WRONG directory.

    Fix: use a specific Silver sentinel file (sell_in_std.csv) as the guard.
    Only a candidate that contains this exact file is accepted as valid.

    Candidates searched (two extra levels to handle nested repo structures):
      {cwd}/logs
      {cwd}/../logs        (one up from phase4_gold/)
      {cwd}/../../logs     (two up — repo root for standard structure)
      {cwd}/../../../logs  (three up — for deeply nested Databricks Repos)
    """
    cwd = os.getcwd()
    candidates = [
        os.path.join(cwd, "logs"),
        os.path.join(cwd, "..", "logs"),
        os.path.join(cwd, "..", "..", "logs"),
        os.path.join(cwd, "..", "..", "..", "logs"),
    ]
    # NOTE: use print() here — log_gold() is not yet defined when this runs at module load
    for c in candidates:
        norm = os.path.normpath(c)
        if os.path.isdir(norm) and os.path.isfile(os.path.join(norm, _SILVER_SENTINEL)):
            print(f"[INIT] LOGS_DIR resolved → {norm}  (sentinel: {_SILVER_SENTINEL})")
            return norm
    # Deep scan: walk up to 5 levels above CWD looking for the sentinel
    for i in range(1, 6):
        segment = os.path.normpath(os.path.join(cwd, *[".."] * i))
        candidate_logs = os.path.join(segment, "logs")
        if os.path.isfile(os.path.join(candidate_logs, _SILVER_SENTINEL)):
            print(f"[INIT] LOGS_DIR resolved (deep scan, {i} levels up) → {candidate_logs}")
            return candidate_logs
    raise FileNotFoundError(
        f"Cannot find logs/{_SILVER_SENTINEL} from CWD={cwd}. "
        "Searched: " + str([os.path.normpath(c) for c in candidates]) + ". "
        "Ensure Phase 3 Silver outputs are present in the repo logs/ directory "
        "and the notebook is run from within the correct Databricks Repo."
    )


def read_silver_csv(path: str, escape_char: str = None):
    """Read a Silver CSV into a Spark DataFrame.

    Why this helper exists:
      spark.read.csv('/Workspace/Users/.../...csv') FAILS on distributed
      Databricks clusters because executors cannot access Workspace paths —
      only the driver can. The fix is to read via pandas on the driver
      (which has full filesystem access) then hand off to Spark.

    For DBFS paths (dbfs:/ or /dbfs/) Spark reads directly — no pandas needed.
    """
    # NOTE: use print() here — log_gold() may not yet be defined if called before LOGS_DIR init
    if path.startswith("/Workspace") or path.startswith("file:/Workspace"):
        print(f"[IO] Reading Silver CSV via pandas bridge (Workspace path): {path}")
        read_kwargs = {"low_memory": False}
        if escape_char:
            read_kwargs["escapechar"] = escape_char
        pdf = pd.read_csv(path, **read_kwargs)
        return spark.createDataFrame(pdf)
    else:
        # DBFS path — Spark reads natively
        print(f"[IO] Reading Silver CSV via Spark (DBFS path): {path}")
        reader = spark.read.option("header", "true").option("inferSchema", "true")
        if escape_char:
            reader = reader.option("escape", escape_char)
        return reader.csv(path)


LOGS_DIR = _resolve_logs_dir()
AUDIT_LOG_PATH = os.path.join(LOGS_DIR, "phase4_standardization_audit_log.txt")



# ── Brand owner classification ────────────────────────────────────────────────
# Source: configs/brand_crosswalk.yaml — danone_brands keys
# Populated at runtime by _load_danone_brands()
_DANONE_BRANDS_SET = set()

def _load_danone_brands():
    """Load Danone brand list from brand_crosswalk.yaml."""
    global _DANONE_BRANDS_SET
    candidates = [
        os.path.join(os.getcwd(), "configs", "brand_crosswalk.yaml"),
        os.path.join(os.getcwd(), "..", "configs", "brand_crosswalk.yaml"),
    ]
    for c in candidates:
        if os.path.exists(c):
            with open(c) as f:
                bx = yaml.safe_load(f)
            _DANONE_BRANDS_SET = {b.upper().strip() for b in bx.get("danone_brands", {}).keys()}
            log_gold("INFO", f"Loaded {len(_DANONE_BRANDS_SET)} Danone brands from brand_crosswalk.yaml", "CONFIG")
            return
    log_gold("WARNING", "brand_crosswalk.yaml not found — DANONE_BRANDS_SET empty", "CONFIG")

# ── KPI registry ─────────────────────────────────────────────────────────────
_KPI_REGISTRY = []

# ── Audit log ────────────────────────────────────────────────────────────────
_LOG_LINES    = []
_BLOCKERS     = []
_WARNINGS     = []

# COMMAND ----------
# MAGIC %md ## 2. Logging Helpers

# COMMAND ----------

def ts():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def log_gold(level, msg, section=""):
    prefix = f"[{ts()}] [{level}]"
    if section:
        prefix += f" [{section}]"
    line = f"{prefix} {msg}"
    _LOG_LINES.append(line)
    print(line)

def gold_blocker(bid, condition, msg, section="VALIDATION"):
    """Register a hard blocker if condition is True."""
    if condition:
        log_gold(f"🚨 BLOCKER {bid}", msg, section)
        _BLOCKERS.append(f"{bid}: {msg}")
    return condition

def gold_warn(wid, condition, msg, section="VALIDATION"):
    """Register a warning if condition is True."""
    if condition:
        log_gold(f"⚠️  WARNING {wid}", msg, section)
        _WARNINGS.append(f"{wid}: {msg}")
    return condition

def gold_passed(pid, msg, section="VALIDATION"):
    log_gold(f"✅ PASS {pid}", msg, section)

def write_audit_log():
    """Flush _LOG_LINES to AUDIT_LOG_PATH."""
    with open(AUDIT_LOG_PATH, "a") as f:
        f.write("\n".join(_LOG_LINES) + "\n")
    log_gold("INFO", f"Audit log flushed → {AUDIT_LOG_PATH}", "AUDIT")

# ── Post-init: echo path resolution into structured log (log_gold now available) ─
log_gold("INFO", f"LOGS_DIR = {LOGS_DIR}", "INIT")
log_gold("INFO", f"AUDIT_LOG = {AUDIT_LOG_PATH}", "INIT")
log_gold("INFO", f"RUN_MODE  = {RUN_MODE}", "INIT")
log_gold("INFO", f"GOLD_START_MONTH = {GOLD_START_MONTH}", "INIT")

# COMMAND ----------
# MAGIC %md ## 3. KPI Utilities

# COMMAND ----------

def safe_divide(numerator_col, denominator_col, zero_result=None):
    """
    Guarded division. Returns zero_result (default NULL) when denominator is
    zero, NULL, or negative (investment guard).
    Usage: df.withColumn("roas", safe_divide(F.col("revenue"), F.col("investment")))
    """
    return F.when(
        (denominator_col.isNull()) | (denominator_col == 0),
        F.lit(zero_result)
    ).otherwise(numerator_col / denominator_col)


def month_trunc(col):
    """Month-truncate a date column → first day of month. Enforces B13."""
    return F.trunc(col, "MM")


def assert_unique_keys(df: DataFrame, keys: list, table_name: str) -> bool:
    """
    Hard blocker B10: right-side join tables must be unique on join keys
    before entering gold_commercial_kpi joins.
    Returns True if unique, raises ValueError if not.
    """
    total = df.count()
    distinct = df.select(keys).distinct().count()
    if total != distinct:
        msg = (f"[{table_name}] UNIQUENESS FAILURE: {total} rows but only "
               f"{distinct} distinct on keys {keys} — fan-out risk!")
        log_gold("🚨 BLOCKER B10", msg, table_name)
        _BLOCKERS.append(f"B10: {msg}")
        raise ValueError(msg)
    log_gold("✅ PASS B10", f"[{table_name}] Unique on {keys} ({total} rows)", table_name)
    return True


def assert_no_join_fanout(left_count: int, joined_df: DataFrame,
                           join_name: str) -> bool:
    """
    Hard blocker B8: joined result must not exceed left (base) row count.
    Call after join: assert_no_join_fanout(base_count, joined_df, 'SI_MASTER_JOIN')
    """
    joined_count = joined_df.count()
    if joined_count > left_count:
        msg = (f"[{join_name}] FAN-OUT DETECTED: base had {left_count} rows, "
               f"joined result has {joined_count} rows — check join keys!")
        log_gold("🚨 BLOCKER B8", msg, join_name)
        _BLOCKERS.append(f"B8: {msg}")
        raise ValueError(msg)
    log_gold("✅ PASS B8", f"[{join_name}] No fan-out ({left_count} → {joined_count})", join_name)
    return True


def validate_expected_columns(df: DataFrame, expected: list, table_name: str):
    """Hard blocker B9: all expected columns must be present."""
    missing = [c for c in expected if c not in df.columns]
    if missing:
        msg = f"[{table_name}] MISSING COLUMNS: {missing}"
        log_gold("🚨 BLOCKER B9", msg, table_name)
        _BLOCKERS.append(f"B9: {msg}")
        raise ValueError(msg)
    gold_passed("B9", f"[{table_name}] All {len(expected)} expected columns present", table_name)


def check_run_mode():
    """Hard blocker B12: SAMPLE mode must not produce production Gold output."""
    if RUN_MODE == "SAMPLE":
        msg = ("RUN_MODE = SAMPLE — Gold KPI outputs are disabled. "
               "Set RUN_MODE = FULL for production Gold build.")
        gold_blocker("B12", True, msg, "RUN_MODE")
        raise RuntimeError(msg)
    log_gold("INFO", f"RUN_MODE = {RUN_MODE} — Gold output enabled", "RUN_MODE")


def check_fecha_month_range(df: DataFrame, table_name: str):
    """Hard blockers B11 + B13: fecha_month in range and month-truncated."""
    # B13 — all dates must be first-of-month
    non_first = df.filter(F.dayofmonth(F.col("fecha_month")) != 1).count()
    gold_blocker("B13", non_first > 0,
                 f"[{table_name}] {non_first} rows have fecha_month not truncated to 1st of month",
                 table_name)
    if non_first == 0:
        gold_passed("B13", f"[{table_name}] All fecha_month values are 1st of month", table_name)

    # B11 — within configured range
    start = F.lit(GOLD_START_MONTH).cast("date")
    oor = df.filter(F.col("fecha_month") < start)
    if GOLD_END_MONTH:
        end = F.lit(GOLD_END_MONTH).cast("date")
        oor = oor.union(df.filter(F.col("fecha_month") > end))
    oor_count = oor.count()
    gold_blocker("B11", oor_count > 0,
                 f"[{table_name}] {oor_count} rows outside GOLD_START_MONTH/GOLD_END_MONTH",
                 table_name)
    if oor_count == 0:
        gold_passed("B11", f"[{table_name}] All fecha_month within configured range", table_name)


def check_no_inf_nan(df: DataFrame, derived_cols: list, table_name: str):
    """Hard blocker B4: no Inf or NaN in derived KPI columns."""
    for col_name in derived_cols:
        if col_name not in df.columns:
            continue
        bad = df.filter(
            F.col(col_name).isNaN() | F.col(col_name).isin([float("inf"), float("-inf")])
        ).count()
        gold_blocker("B4", bad > 0,
                     f"[{table_name}] {bad} Inf/NaN values in column '{col_name}'",
                     table_name)
        if bad == 0:
            gold_passed("B4", f"[{table_name}] No Inf/NaN in '{col_name}'", table_name)

# COMMAND ----------
# MAGIC %md ## 4. Save / Output Helpers

# COMMAND ----------

def save_gold_df(df: DataFrame, name: str, section: str = ""):
    """
    Save a Gold KPI DataFrame to DBFS (primary) and log.
    name: e.g. 'gold_sell_in_kpi' — .csv appended automatically.
    B16: large commercial KPI CSVs are NOT committed to repo.
    """
    dbfs_path = f"{DBFS_GOLD_ROOT}/{name}"
    try:
        dbutils.fs.mkdirs(DBFS_GOLD_ROOT)
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(dbfs_path)
        log_gold("INFO", f"Saved → {dbfs_path} ({df.count()} rows)", section or name)
    except NameError:
        # dbutils not available (local/test mode)
        local_path = os.path.join(LOGS_DIR, f"{name}.csv")
        df.toPandas().to_csv(local_path, index=False)
        log_gold("INFO", f"[LOCAL MODE] Saved → {local_path} ({df.count()} rows)", section or name)


def register_gold_metric(name: str, source: str, formula: str,
                          unit: str, grain: str):
    """Register a KPI in the KPI registry (written to logs/phase4_kpi_registry.csv)."""
    _KPI_REGISTRY.append({
        "name": name, "source": source, "formula": formula,
        "unit": unit, "grain": grain,
        "registered_at": ts()
    })


def flush_kpi_registry():
    """Write accumulated KPI registry to logs/phase4_kpi_registry.csv."""
    path = os.path.join(LOGS_DIR, "phase4_kpi_registry.csv")
    pd.DataFrame(_KPI_REGISTRY).to_csv(path, index=False)
    log_gold("INFO", f"KPI registry flushed → {path} ({len(_KPI_REGISTRY)} metrics)", "REGISTRY")


def load_phase_config():
    """Load dq_thresholds.yaml and return the config dict."""
    candidates = [
        os.path.join(os.getcwd(), "configs", "dq_thresholds.yaml"),
        os.path.join(os.getcwd(), "..", "configs", "dq_thresholds.yaml"),
    ]
    for c in candidates:
        if os.path.exists(c):
            with open(c) as f:
                cfg = yaml.safe_load(f)
            log_gold("INFO", f"Loaded dq_thresholds.yaml from {c}", "CONFIG")
            return cfg
    log_gold("WARNING", "dq_thresholds.yaml not found — using default thresholds", "CONFIG")
    return {
        "sell_out_coverage_low_threshold": 0.70,
        "sell_out_coverage_medium_threshold": 0.85,
    }


def coverage_level_expr(store_count_col, low_t=0.70, med_t=0.85):
    """
    Returns a Spark Column expression for coverage_level.
    Thresholds sourced from dq_thresholds.yaml.
    """
    return (
        F.when(store_count_col < F.lit(low_t), "LOW")
         .when(store_count_col < F.lit(med_t), "MEDIUM")
         .otherwise("HIGH")
    )

# COMMAND ----------
# MAGIC %md ## 5. Initialise

# COMMAND ----------

log_gold("INFO", "=" * 72, "INIT")
log_gold("INFO", "PHASE 4 GOLD KPI UTILS — INITIALISED", "INIT")
log_gold("INFO", f"RUN_MODE         = {RUN_MODE}", "INIT")
log_gold("INFO", f"GOLD_START_MONTH = {GOLD_START_MONTH}", "INIT")
log_gold("INFO", f"GOLD_END_MONTH   = {GOLD_END_MONTH}", "INIT")
log_gold("INFO", f"DBFS_GOLD_ROOT   = {DBFS_GOLD_ROOT}", "INIT")
log_gold("INFO", f"LOGS_DIR         = {LOGS_DIR}", "INIT")
log_gold("INFO", "B14 PRE-CONFIRMED: zero Snowflake write operations in Phase 4 notebooks", "SECURITY")
log_gold("INFO", "B15 PRE-CONFIRMED: zero production Snowflake mutation statements", "SECURITY")
log_gold("INFO", "=" * 72, "INIT")

_load_danone_brands()
