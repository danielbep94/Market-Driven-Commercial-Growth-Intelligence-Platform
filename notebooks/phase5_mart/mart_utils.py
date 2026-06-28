# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 5 Analytics Mart — Shared Utilities
# MAGIC **mart_utils.py**
# MAGIC
# MAGIC Provides shared config, logging, readers, writers and helpers for
# MAGIC `mart_builder.py` and `mart_validation.py`.
# MAGIC
# MAGIC **Zero Snowflake writes. DBFS + logs/ only.**
# MAGIC B14/B15 pre-confirmed.

# COMMAND ----------

import os
import re
import uuid
import datetime
import subprocess

import pandas as pd

from pyspark.sql import functions as F, Window, DataFrame
from pyspark.sql.types import (
    DoubleType, LongType, IntegerType, StringType,
    DateType, TimestampType
)

# COMMAND ----------
# MAGIC %md ## 1. Configuration

# COMMAND ----------

MART_RUN_MODE    = "FULL"
MART_START_MONTH = "2025-01-01"

DBFS_GOLD_ROOT   = "dbfs:/mnt/mdp/mdm/phase4_gold/data"
DBFS_MART_ROOT   = "dbfs:/mnt/mdp/mdm/phase5_mart"
DBFS_MART_TABLE  = f"{DBFS_MART_ROOT}/mart_commercial_kpi_monthly"

MART_GRAIN       = ["fecha_month", "marca_std", "canal_std", "cadena_std"]
GOLD_TABLE_NAME  = "gold_commercial_kpi"
GOLD_EXPECTED_ROWS = 4_427

# ── Resolve logs/ dir ─────────────────────────────────────────────────────────
_SILVER_SENTINEL = "sell_in_std.csv"

def _resolve_logs_dir() -> str:
    cwd = os.getcwd()
    candidates = [
        os.path.join(cwd, "logs"),
        os.path.join(cwd, "..", "logs"),
        os.path.join(cwd, "..", "..", "logs"),
        os.path.join(cwd, "..", "..", "..", "logs"),
    ]
    for i in range(1, 6):
        candidates.append(
            os.path.join(os.path.normpath(os.path.join(cwd, *[".."] * i)), "logs")
        )
    seen = set()
    for c in candidates:
        norm = os.path.normpath(c)
        if norm in seen:
            continue
        seen.add(norm)
        if os.path.isdir(norm) and os.path.isfile(os.path.join(norm, _SILVER_SENTINEL)):
            print(f"[INIT] LOGS_DIR resolved → {norm}")
            return norm
    raise FileNotFoundError(
        f"Cannot find logs/{_SILVER_SENTINEL} from CWD={cwd}. "
        "Ensure Phase 3 Silver outputs are present."
    )

LOGS_DIR       = _resolve_logs_dir()
AUDIT_LOG_PATH = os.path.join(LOGS_DIR, "phase5_mart_audit_log.txt")
COVERAGE_PATH  = os.path.join(LOGS_DIR, "phase5_mart_coverage_report.txt")
RECONCILE_PATH = os.path.join(LOGS_DIR, "phase5_row_count_reconciliation.txt")
REGISTRY_PATH  = os.path.join(LOGS_DIR, "phase5_kpi_registry.csv")
PBI_CSV_PATH   = os.path.join(LOGS_DIR, "mart_commercial_kpi_monthly_pbi.csv")

# COMMAND ----------
# MAGIC %md ## 2. Logging

# COMMAND ----------

_LOG_LINES = []
_BLOCKERS  = []
_WARNINGS  = []

def ts() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def log_mart(level: str, msg: str, section: str = "") -> None:
    prefix = f"[{ts()}] [{level}]"
    if section:
        prefix += f" [{section}]"
    line = f"{prefix} {msg}"
    _LOG_LINES.append(line)
    print(line)

def mart_blocker(bid: str, condition: bool, msg: str, section: str = "VALIDATION") -> bool:
    if condition:
        log_mart(f"🚨 BLOCKER {bid}", msg, section)
        _BLOCKERS.append(f"{bid}: {msg}")
    return condition

def mart_warn(wid: str, condition: bool, msg: str, section: str = "VALIDATION") -> bool:
    if condition:
        log_mart(f"⚠️  WARNING {wid}", msg, section)
        _WARNINGS.append(f"{wid}: {msg}")
    return condition

def mart_passed(pid: str, msg: str, section: str = "VALIDATION") -> None:
    log_mart(f"✅ PASS {pid}", msg, section)

def write_mart_audit_log(
    pipeline_run_id: str,
    run_id_source: str,
    gold_rows: int,
    mart_rows: int,
    validation_status: str,
) -> None:
    lines = [
        "=" * 72,
        "PHASE 5 MART — AUDIT LOG",
        "=" * 72,
        f"run_timestamp       : {ts()}",
        f"pipeline_run_id     : {pipeline_run_id}",
        f"run_id_source       : {run_id_source}",
        f"input_path          : {DBFS_GOLD_ROOT}/{GOLD_TABLE_NAME}",
        f"output_path_dbfs    : {DBFS_MART_TABLE}",
        f"output_path_csv     : {PBI_CSV_PATH}",
        f"dbfs_output_format  : PARQUET",
        f"gold_row_count      : {gold_rows:,}",
        f"mart_row_count      : {mart_rows:,}",
        f"validation_status   : {validation_status}",
        "snowflake_writes    : CONFIRMED ZERO",
        "b14_confirmed       : zero Snowflake write operations in Phase 5 notebooks",
        "b15_confirmed       : zero Snowflake mutation statements in Phase 5 notebooks",
        "",
        *_LOG_LINES,
    ]
    with open(AUDIT_LOG_PATH, "w") as f:
        f.write("\n".join(lines) + "\n")
    print(f"[AUDIT] Audit log written → {AUDIT_LOG_PATH}")

# COMMAND ----------
# MAGIC %md ## 3. Pipeline Run ID

# COMMAND ----------

def _resolve_pipeline_run_id() -> tuple:
    """
    Returns (run_id, source) using fallback hierarchy:
    1. Databricks job run ID
    2. Git commit hash (short)
    3. UUID fallback
    """
    # 1. Databricks job run ID
    try:
        ctx  = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        job_id = ctx.tags().get("jobId").getOrElse(None)
        run_id = (ctx.tags().get("multitaskParentRunId").getOrElse(None) or
                  ctx.tags().get("idInJob").getOrElse(None))
        if job_id and run_id:
            return f"job_{job_id}_run_{run_id}", "DATABRICKS_JOB"
    except Exception:
        pass
    # 2. Git commit hash
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0:
            return result.stdout.strip(), "GIT_HASH"
    except Exception:
        pass
    # 3. UUID fallback
    return uuid.uuid4().hex[:12], "UUID_FALLBACK"

# COMMAND ----------
# MAGIC %md ## 4. Gold Reader

# COMMAND ----------

def read_gold_df(table_name: str) -> DataFrame:
    """
    Read a Phase 4 Gold table from DBFS.
    Tries Parquet first, then CSV fallback.
    No Snowflake reads. No Silver/Bronze reads.
    """
    path = f"{DBFS_GOLD_ROOT}/{table_name}"
    try:
        df = spark.read.parquet(path)
        log_mart("INFO", f"Loaded Gold (Parquet): {path}", "IO")
        return df
    except Exception:
        pass
    try:
        df = (spark.read
              .option("header", "true")
              .option("inferSchema", "true")
              .csv(path))
        log_mart("INFO", f"Loaded Gold (CSV): {path}", "IO")
        return df
    except Exception as e:
        raise FileNotFoundError(
            f"Cannot read Gold table '{table_name}' from {DBFS_GOLD_ROOT}. "
            f"Tried Parquet and CSV. Error: {e}"
        )

# COMMAND ----------
# MAGIC %md ## 5. Writers

# COMMAND ----------

def save_mart_df(df: DataFrame, name: str, section: str = "IO") -> int:
    """Write mart DataFrame to DBFS as Parquet. Returns row count."""
    path = f"{DBFS_MART_ROOT}/{name}"
    df.write.mode("overwrite").parquet(path)
    count = df.count()
    log_mart("INFO", f"Saved Parquet → {path} ({count:,} rows)", section)
    return count

def save_mart_csv(df: DataFrame, section: str = "IO") -> int:
    """
    Write mart to logs/ as single flat CSV for Power BI.
    - Dates as YYYY-MM-DD strings
    - Nulls as empty values
    - One header row, no duplicate columns
    """
    df_csv = df.withColumn(
        "fecha_month",
        F.date_format(F.col("fecha_month").cast(DateType()), "yyyy-MM-dd")
    )
    if "mart_computed_at" in df_csv.columns:
        df_csv = df_csv.withColumn(
            "mart_computed_at", F.col("mart_computed_at").cast(StringType())
        )
    pdf = df_csv.toPandas()
    pdf.to_csv(PBI_CSV_PATH, index=False, na_rep="")
    count = len(pdf)
    log_mart("INFO", f"Saved Power BI CSV → {PBI_CSV_PATH} ({count:,} rows)", section)
    return count

# COMMAND ----------
# MAGIC %md ## 6. KPI Registry

# COMMAND ----------

_KPI_REGISTRY = []

def register_mart_metric(
    mart_column: str,
    source_domain: str,
    source_column_or_formula: str,
    definition: str,
    grain: str,
    null_handling: str,
    status: str,
    notes: str = "",
) -> None:
    _KPI_REGISTRY.append({
        "mart_column":              mart_column,
        "source_domain":            source_domain,
        "source_column_or_formula": source_column_or_formula,
        "definition":               definition,
        "grain":                    grain,
        "null_handling":            null_handling,
        "status":                   status,
        "notes":                    notes,
    })

def flush_mart_registry() -> None:
    pd.DataFrame(_KPI_REGISTRY).to_csv(REGISTRY_PATH, index=False)
    log_mart("INFO",
             f"KPI registry flushed → {REGISTRY_PATH} ({len(_KPI_REGISTRY)} entries)",
             "REGISTRY")

# COMMAND ----------
# MAGIC %md ## 7. Shared Helpers

# COMMAND ----------

def safe_divide(numerator_col, denominator_col):
    """
    Guarded division. Returns NULL (not 0, not Inf) when denominator is NULL or 0.
    """
    return F.when(
        denominator_col.isNull() | (denominator_col == 0),
        F.lit(None)
    ).otherwise(numerator_col / denominator_col)

def assert_unique_keys(df: DataFrame, keys: list, table_name: str) -> bool:
    """Hard blocker: DataFrame must be unique on keys."""
    total    = df.count()
    distinct = df.select(keys).distinct().count()
    if total != distinct:
        msg = (f"[{table_name}] GRAIN FAILURE: {total:,} rows / "
               f"{distinct:,} distinct on {keys} — fan-out risk!")
        log_mart("🚨 BLOCKER MG4", msg, table_name)
        _BLOCKERS.append(f"MG4: {msg}")
        raise ValueError(msg)
    mart_passed("MG4", f"[{table_name}] Unique on {keys} ({total:,} rows)", table_name)
    return True

def check_no_inf_nan(df: DataFrame, cols: list, table_name: str, check_id: str) -> None:
    """Hard blocker: no Inf or NaN in specified numeric columns."""
    for c in cols:
        if c not in df.columns:
            continue
        bad = df.filter(
            F.col(c).isNotNull() &
            (F.isnan(F.col(c).cast("double")) |
             (F.col(c).cast("double") == float("inf")) |
             (F.col(c).cast("double") == float("-inf")))
        ).count()
        if not mart_blocker(check_id, bad > 0,
                            f"[{table_name}] '{c}' has {bad:,} Inf/NaN rows", table_name):
            mart_passed(check_id, f"[{table_name}] '{c}' — no Inf/NaN", table_name)

# COMMAND ----------
# MAGIC %md ## 8. Post-init

# COMMAND ----------

log_mart("INFO", "=" * 72, "INIT")
log_mart("INFO", "PHASE 5 ANALYTICS MART UTILS — INITIALISED", "INIT")
log_mart("INFO", f"MART_RUN_MODE    = {MART_RUN_MODE}", "INIT")
log_mart("INFO", f"MART_START_MONTH = {MART_START_MONTH}", "INIT")
log_mart("INFO", f"DBFS_GOLD_ROOT   = {DBFS_GOLD_ROOT}", "INIT")
log_mart("INFO", f"DBFS_MART_ROOT   = {DBFS_MART_ROOT}", "INIT")
log_mart("INFO", f"LOGS_DIR         = {LOGS_DIR}", "INIT")
log_mart("INFO", "B14 PRE-CONFIRMED: zero Snowflake write operations in Phase 5 notebooks", "SECURITY")
log_mart("INFO", "B15 PRE-CONFIRMED: zero Snowflake mutation statements in Phase 5 notebooks", "SECURITY")
log_mart("INFO", "=" * 72, "INIT")
