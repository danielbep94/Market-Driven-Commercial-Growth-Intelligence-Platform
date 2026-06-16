# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Execution Logger — Phase 1 Shared Utility
# MAGIC
# MAGIC **Usage (in any Phase 1 notebook):**
# MAGIC ```python
# MAGIC %run ../utils/execution_logger
# MAGIC # ... notebook body ...
# MAGIC write_execution_log(
# MAGIC     notebook_id    = "00b_snowflake_discovery",
# MAGIC     source_name    = "DATA_SELL_OUT",    # None for infra notebooks
# MAGIC     status         = "SUCCESS",           # SUCCESS | PARTIAL | ERROR | SKIPPED
# MAGIC     duration_sec   = _nb_duration,
# MAGIC     errors         = _errors,             # list of ErrorRecord dicts
# MAGIC     warnings       = _warnings,           # list of strings
# MAGIC     metrics        = _metrics,            # notebook-specific KPIs
# MAGIC     output_files   = _output_files,       # list of paths written
# MAGIC     steps_passed   = _steps_passed,
# MAGIC     steps_failed   = _steps_failed,
# MAGIC     environment    = ENVIRONMENT,
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Storage targets:**
# MAGIC - DBFS JSON : `dbfs:/mgi/phase1_logs/<notebook_id>/YYYYMMDD_HHMMSS_<SOURCE>_<STATUS>.json`
# MAGIC - Delta table: `mgi_metadata.execution_log` (Hive metastore, appended, never overwritten)

# COMMAND ----------

import json
import uuid
from datetime import datetime


# ── Constants ────────────────────────────────────────────────────────────────
DBFS_LOG_ROOT   = "dbfs:/mgi/phase1_logs"
DELTA_SCHEMA    = "mgi_metadata"          # Hive metastore database
DELTA_TABLE     = "execution_log"         # table name
DELTA_FULL_NAME = f"{DELTA_SCHEMA}.{DELTA_TABLE}"


# ── Bootstrap: ensure Hive database exists ───────────────────────────────────
def _ensure_log_schema():
    """Create mgi_metadata Hive database if it does not yet exist."""
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {DELTA_SCHEMA}")
    except Exception as _e:
        print(f"⚠️  Could not create Hive database '{DELTA_SCHEMA}': {_e}")


# ── Helper: make error record dict ───────────────────────────────────────────
def make_error(step, category, message, raw_exception="", resolution="", is_blocking=False,
               severity="HIGH"):
    """
    Build a structured ErrorRecord dict for inclusion in the errors list.

    Categories (from validation plan §2.2):
        SECRET_RETRIEVAL_FAILED, CONNECTION_FAILED, SOURCE_NOT_CONFIGURED,
        TABLE_NOT_FOUND, SCHEMA_MISMATCH, GRAIN_NOT_RESOLVED,
        TEMPORAL_CAST_FAILURE, NULL_RATE_CRITICAL, JOIN_KEY_MISMATCH,
        TIMEOUT, INFORMATION_SCHEMA_UNAVAILABLE, BUSINESS_KEY_NOT_FOUND,
        DELTA_WRITE_FAILED, DQ_RULE_WARNING, GLOSSARY_SEED_MISSING
    """
    return {
        "error_id":      str(uuid.uuid4())[:8],
        "step":          step,
        "category":      category,
        "severity":      severity,
        "message":       str(message)[:300],
        "raw_exception": str(raw_exception)[:500],
        "resolution":    resolution,
        "is_blocking":   is_blocking,
    }


# ── Main writer ───────────────────────────────────────────────────────────────
def write_execution_log(
    notebook_id,
    source_name,
    status,
    duration_sec,
    errors,
    warnings,
    metrics,
    output_files,
    steps_passed   = 0,
    steps_failed   = 0,
    environment    = "dev",
):
    """
    Persist one execution log record to DBFS (JSON) and Delta (Hive).

    Parameters
    ----------
    notebook_id   : str   e.g. "00b_snowflake_discovery"
    source_name   : str   e.g. "DATA_SELL_OUT"  —  None for infra notebooks
    status        : str   "SUCCESS" | "PARTIAL" | "ERROR" | "SKIPPED"
    duration_sec  : float wall-clock seconds
    errors        : list  of make_error() dicts
    warnings      : list  of plain strings
    metrics       : dict  notebook-specific KPIs (row_count, score, gate_status, etc.)
    output_files  : list  of path strings written by this notebook
    steps_passed  : int   checkpoint count that completed without exception
    steps_failed  : int   checkpoint count that raised an exception
    environment   : str   "dev" | "prod"

    Returns
    -------
    dict  — the full log record that was written
    """
    run_at  = datetime.utcnow().isoformat() + "Z"
    run_id  = str(uuid.uuid4())[:8]

    # Derive Databricks job run ID when available
    try:
        job_run_id = (
            dbutils.notebook.entry_point.getCurrentBindings()
                   .get("run_id", run_id)
        )
    except Exception:
        job_run_id = run_id

    record = {
        "notebook":         str(notebook_id),
        "run_at":           run_at,
        "run_id":           job_run_id,
        "environment":      str(environment),
        "source_name":      str(source_name) if source_name else "infra",
        "status":           str(status),
        "exit_code":        str(status),
        "duration_seconds": round(float(duration_sec), 1),
        "steps_passed":     int(steps_passed),
        "steps_failed":     int(steps_failed),
        "errors":           errors or [],
        "warnings":         warnings or [],
        "metrics":          metrics or {},
        "output_files":     output_files or [],
    }

    # ── 1. Write DBFS JSON ────────────────────────────────────────────────────
    try:
        dbutils.fs.mkdirs(f"{DBFS_LOG_ROOT}/{notebook_id}")
        ts      = run_at[:19].replace("-", "").replace("T", "_").replace(":", "")
        src_tag = (source_name or "infra").replace(" ", "_").upper()
        fname   = f"{ts}_{src_tag}_{status}.json"
        dbfs_path = f"{DBFS_LOG_ROOT}/{notebook_id}/{fname}"
        dbutils.fs.put(dbfs_path, json.dumps(record, indent=2, default=str), overwrite=False)
        print(f"  📋 Log → DBFS : {dbfs_path}")
    except Exception as _e:
        dbfs_path = "DBFS_WRITE_FAILED"
        print(f"  ⚠️  DBFS log write failed: {_e}")

    # ── 2. Append to Delta table (mgi_metadata.execution_log) ────────────────
    try:
        _ensure_log_schema()
        from pyspark.sql import Row
        import pyspark.sql.functions as F

        # Flatten nested fields to strings for Delta compatibility
        flat = {
            "notebook":         record["notebook"],
            "run_at":           record["run_at"],
            "run_id":           record["run_id"],
            "environment":      record["environment"],
            "source_name":      record["source_name"],
            "status":           record["status"],
            "exit_code":        record["exit_code"],
            "duration_seconds": str(record["duration_seconds"]),
            "steps_passed":     str(record["steps_passed"]),
            "steps_failed":     str(record["steps_failed"]),
            "errors_json":      json.dumps(record["errors"], default=str),
            "warnings_json":    json.dumps(record["warnings"], default=str),
            "metrics_json":     json.dumps(record["metrics"], default=str),
            "output_files_json": json.dumps(record["output_files"], default=str),
            "dbfs_log_path":    dbfs_path,
        }

        sdf = spark.createDataFrame([flat])
        sdf = sdf.withColumn("_partition_date", F.to_date(F.lit(run_at[:10])))
        sdf.write.format("delta") \
            .mode("append") \
            .partitionBy("_partition_date") \
            .saveAsTable(DELTA_FULL_NAME)
        print(f"  📋 Log → Delta: {DELTA_FULL_NAME}")

    except Exception as _e:
        print(f"  ⚠️  Delta log append failed (DBFS log still persisted): {_e}")
        record["_delta_write_error"] = str(_e)[:200]

    return record


# COMMAND ----------
print("✅ execution_logger loaded — write_execution_log() and make_error() available.")
