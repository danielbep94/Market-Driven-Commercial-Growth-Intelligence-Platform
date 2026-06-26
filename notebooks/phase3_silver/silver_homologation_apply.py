# Databricks notebook source
# =============================================================================
# PHASE 3 — SHARED UTILITIES: silver_homologation_apply.py
# =============================================================================
# PURPOSE:
#   Central entrypoint %run'd by every Phase 3 silver notebook.
#   Provides:
#     - Snowflake credentials + readers (same pattern as Phase 2)
#     - Dual-write log helpers (DBFS + repo logs/)
#     - load_mapping_csv()   — loads CSV mapping with key uniqueness assertion (R14)
#     - load_yaml_config()   — loads YAML configs (R12)
#     - register_join()      — JOIN_REGISTRY append (Change #7)
#     - assert_no_prohibited_join() — scans JOIN_REGISTRY for violations (A1-A3)
#     - assert_row_count_exact()    — exact left-join row count check (R15)
#     - quarantine()         — accumulates unmatched rows for quarantine report
#     - flush_quarantine()   — writes combined quarantine report
#     - phase3_final_summary() — prints gate status (CLEAR / WARNINGS / BLOCKED)
#
# ARCHITECTURAL CONSTANTS (locked in Phase 2 — DO NOT change):
#   ACTIVE_CATALOG_FILTER = "SKU_EAN_COD IS NOT NULL"   (R4)
#   DB_PRD_MEX / DB_PRD_MDP                              (R1-R11)
#
# APPROVED STRUCTURAL HARDCODING (R9 — not a R12 violation):
#   NULL::VARCHAR AS cadena_std  in mkt_off_std notebooks
#   This is an architectural contract, not a mapping rule.
# =============================================================================

# COMMAND ----------

import os, importlib.util, datetime, pathlib
from pyspark.sql import functions as F
from pyspark.sql.types import *

# =============================================================================
# 1. SNOWFLAKE CREDENTIALS
# =============================================================================

_current_dir = os.getcwd()
_creds_path  = os.path.normpath(
    os.path.join(_current_dir, "../..", "configs", "snowflake_creds.py"))

if not os.path.exists(_creds_path):
    raise FileNotFoundError(
        f"configs/snowflake_creds.py NOT FOUND at {_creds_path}. "
        "Ensure the repo is checked out and the file exists.")

_spec = importlib.util.spec_from_file_location("snowflake_creds", _creds_path)
_m    = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_m)

SF_URL     = "danonenam.east-us-2.azure.snowflakecomputing.com"
DB_PRD_MEX = "PRD_MEX"
DB_PRD_MDP = "PRD_MDP"

def get_sf_options(database: str) -> dict:
    _mdp_user = getattr(_m, "SF_MDP_USER",     None)
    _mdp_pwd  = getattr(_m, "SF_MDP_PASSWORD", None)
    profiles  = {
        "PRD_MEX": {
            "sfURL":       SF_URL,
            "sfUser":      _m.SF_MEX_USER,
            "sfPassword":  _m.SF_MEX_PASSWORD,
            "sfWarehouse": getattr(_m, "SF_MEX_WH",   "PRD_MEX_ANL_WH"),
            "sfRole":      getattr(_m, "SF_MEX_ROLE", "PRD_MEX_READER"),
        },
        "PRD_MDP": {
            "sfURL":       SF_URL,
            "sfUser":      _mdp_user or dbutils.secrets.get(
                               "DAN-AM-P-KVT800-R-MDP-DB", "snowflake-user"),
            "sfPassword":  _mdp_pwd  or dbutils.secrets.get(
                               "DAN-AM-P-KVT800-R-MDP-DB", "snowflake-password"),
            "sfWarehouse": getattr(_m, "SF_MDP_WH",   "PRD_MDP_ANL_WH"),
            "sfRole":      getattr(_m, "SF_MDP_ROLE", "PRD_MDP"),
        },
    }
    if database not in profiles:
        raise ValueError(f"No Snowflake profile for '{database}'. "
                         f"Valid: {list(profiles.keys())}")
    return dict(profiles[database])

def run_sf(database: str, sql: str):
    """Execute a Snowflake SQL query and return a Spark DataFrame."""
    return (spark.read
            .format("net.snowflake.spark.snowflake")
            .options(**get_sf_options(database))
            .option("sfDatabase", database)
            .option("query", sql)
            .load())

# COMMAND ----------

# =============================================================================
# 2. PATHS & DUAL-WRITE LOG INFRASTRUCTURE
# =============================================================================

REPO_ROOT     = pathlib.Path(_current_dir).parent.parent
REPO_LOGS_DIR = str(REPO_ROOT / "logs")
os.makedirs(REPO_LOGS_DIR, exist_ok=True)

DBFS_ROOT = "dbfs:/mnt/mdp/mdm/phase3_std"
dbutils.fs.mkdirs(DBFS_ROOT)

# R4 — locked in Phase 2. All active-catalog queries must use this exact filter.
ACTIVE_CATALOG_FILTER = "SKU_EAN_COD IS NOT NULL"

_LOG_LINES     = []
_HARD_BLOCKERS = []
_WARNINGS      = []

def ts() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def log(level: str, msg: str, section: str = ""):
    prefix = f"[{ts()}] [{level}]"
    if section:
        prefix += f" [{section}]"
    line = f"{prefix} {msg}"
    _LOG_LINES.append(line)
    print(line)

def flush_log(filename: str = "phase3_standardization_audit_log.txt"):
    """Dual-write audit log to DBFS and repo logs/."""
    content = "\n".join(_LOG_LINES)
    dbutils.fs.put(f"{DBFS_ROOT}/{filename}", content, overwrite=True)
    repo_path = os.path.join(REPO_LOGS_DIR, filename)
    with open(repo_path, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"📄 Audit log → DBFS: {DBFS_ROOT}/{filename}")
    print(f"📄 Audit log → REPO: {repo_path}")

def save_df(df, name: str, section: str = "", row_limit: int = 100_000):
    """Dual-write DataFrame: partitioned CSV to DBFS + flat CSV to repo logs/."""
    dbfs_path = f"{DBFS_ROOT}/{name}"
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(dbfs_path)
    repo_path = os.path.join(REPO_LOGS_DIR, name)
    (df.limit(row_limit).toPandas().to_csv(repo_path, index=False, encoding="utf-8"))
    log("INFO", f"Saved → DBFS: {dbfs_path}  |  REPO: {repo_path}", section)

def blocker(cond: bool, msg: str, section: str = "") -> bool:
    if cond:
        log("🚨 BLOCKER", msg, section)
        _HARD_BLOCKERS.append(msg)
    return cond

def warn(cond: bool, msg: str, section: str = "") -> bool:
    if cond:
        log("⚠️  WARNING", msg, section)
        _WARNINGS.append(msg)
    return cond

def passed(msg: str, section: str = ""):
    log("✅ PASS", msg, section)

# COMMAND ----------

# =============================================================================
# 3. MAPPING LOADERS  (R12 — all rules from CSV/YAML, never hardcoded in SQL)
# =============================================================================

def load_mapping_csv(path: str, key_col: str, section: str = "MAPPING_LOADER"):
    """
    Load a CSV mapping file as a Spark DataFrame.
    ASSERTS uniqueness on key_col before returning. (R14 — Change #5)

    A non-unique mapping table would cause silent fanout in any join.
    Raises BLOCKER if duplicates found; returns the full (non-deduped)
    DataFrame so the caller can inspect violations.

    Args:
        path:    Absolute or repo-relative path (relative to REPO_ROOT).
        key_col: Column that must be unique (the join key).
        section: Log section label.
    """
    if not os.path.isabs(path):
        path = str(REPO_ROOT / path)

    if not os.path.exists(path):
        warn(True,
             f"Mapping file not found: {path}. "
             "Returning empty DataFrame — all joins from this mapping will produce NULLs.",
             section)
        return spark.createDataFrame([], schema=StructType([]))

    df = (spark.read
          .option("header", "true")
          .option("encoding", "UTF-8")
          .csv(f"file://{path}"))

    total_rows  = df.count()
    unique_rows = df.dropDuplicates([key_col]).count()
    dup_count   = total_rows - unique_rows

    if dup_count > 0:
        blocker(True,
                f"MAPPING UNIQUENESS VIOLATION — {os.path.basename(path)}: "
                f"{dup_count:,} duplicate values on key_col='{key_col}'. "
                f"Fix the mapping file before joining. "
                f"Total={total_rows:,}, unique={unique_rows:,}.",
                section)
    else:
        log("INFO",
            f"Mapping loaded: {os.path.basename(path)} — "
            f"{total_rows:,} rows, all unique on '{key_col}'",
            section)
    return df

def load_yaml_config(path: str) -> dict:
    """Load a YAML configuration file. (R12)"""
    import yaml
    if not os.path.isabs(path):
        path = str(REPO_ROOT / path)
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

# COMMAND ----------

# =============================================================================
# 4. JOIN REGISTRY  (Change #7)
# =============================================================================

JOIN_REGISTRY = []  # list[dict]

def register_join(notebook:  str,
                  left:      str,
                  right:     str,
                  join_key:  str,
                  join_type: str,
                  notes:     str = ""):
    """
    Register every Spark join so phase3_mdm_validation.py can run
    assert_no_prohibited_join() without scanning source code. (A1, A2, A3)
    Call IMMEDIATELY after every df.join() call.
    """
    entry = {
        "notebook":      notebook,
        "left":          left,
        "right":         right,
        "join_key":      join_key,
        "join_type":     join_type,
        "notes":         notes,
        "registered_at": ts(),
    }
    JOIN_REGISTRY.append(entry)
    log("INFO",
        f"JOIN: {notebook} | {left} ↔ {right} ON [{join_key}] ({join_type})",
        "JOIN_REGISTRY")

def assert_no_prohibited_join(prohibited_keys: list,
                               rule_label:      str,
                               section:         str):
    """
    Scan JOIN_REGISTRY for joins using any prohibited key (case-insensitive
    substring match on join_key field). Raises BLOCKER per violation.

    Args:
        prohibited_keys: Column name substrings to prohibit,
                         e.g. ["UPC", "EAN", "SKU_EAN"]
        rule_label:      Rule name for the log, e.g. "R6 MKT_ON no UPC join"
        section:         Log section label
    """
    violations = 0
    for entry in JOIN_REGISTRY:
        for pk in prohibited_keys:
            if pk.upper() in entry["join_key"].upper():
                blocker(True,
                        f"PROHIBITED JOIN — {rule_label}: "
                        f"notebook='{entry['notebook']}' "
                        f"{entry['left']} ↔ {entry['right']} "
                        f"ON [{entry['join_key']}]. "
                        f"Prohibited pattern: '{pk}'.",
                        section)
                violations += 1
    if violations == 0:
        passed(f"{rule_label}: No prohibited join found. "
               f"({len(JOIN_REGISTRY)} joins in registry scanned.)", section)

def get_join_registry_df():
    """Serialize JOIN_REGISTRY to a Spark DataFrame for saving to logs."""
    schema = StructType([
        StructField("notebook",      StringType(), True),
        StructField("left",          StringType(), True),
        StructField("right",         StringType(), True),
        StructField("join_key",      StringType(), True),
        StructField("join_type",     StringType(), True),
        StructField("notes",         StringType(), True),
        StructField("registered_at", StringType(), True),
    ])
    if not JOIN_REGISTRY:
        return spark.createDataFrame([], schema=schema)
    return spark.createDataFrame(JOIN_REGISTRY, schema=schema)

# COMMAND ----------

# =============================================================================
# 5. ROW COUNT ASSERTION  (R15 — Change #4)
# =============================================================================

def assert_row_count_exact(df_before,
                            df_after,
                            label:             str,
                            section:           str,
                            documented_filter: str = None):
    """
    Assert that a left join preserved exact row count from the left side.

    - n_after == n_before  → PASS
    - n_after >  n_before  → BLOCKER (fanout: mapping table has duplicate keys)
    - n_after <  n_before  + documented_filter=None  → BLOCKER (silent drop)
    - n_after <  n_before  + documented_filter=str   → WARNING (documented drop)

    Args:
        df_before:          Left DataFrame before join
        df_after:           Result DataFrame after join
        label:              Step description for the log
        section:            Log section label
        documented_filter:  String description of any intentional row filter
                            (turns BLOCKER into WARNING if provided)

    Returns:
        (n_before, n_after) for row-count reconciliation report
    """
    n_before = df_before.count()
    n_after  = df_after.count()

    if n_after == n_before:
        passed(f"{label}: row count preserved — {n_after:,} rows.", section)

    elif n_after > n_before:
        blocker(True,
                f"ROW COUNT FANOUT in '{label}': "
                f"before={n_before:,}, after={n_after:,} "
                f"(+{n_after - n_before:,}). "
                "Mapping table has duplicate join keys. "
                "Check load_mapping_csv uniqueness assertion for this join.",
                section)

    else:  # n_after < n_before
        if documented_filter:
            warn(True,
                 f"ROW COUNT REDUCED in '{label}': "
                 f"before={n_before:,}, after={n_after:,} "
                 f"(-{n_before - n_after:,}). "
                 f"DOCUMENTED FILTER: {documented_filter}",
                 section)
        else:
            blocker(True,
                    f"SILENT ROW DROP in '{label}': "
                    f"before={n_before:,}, after={n_after:,} "
                    f"(-{n_before - n_after:,}). "
                    "A left join must never silently drop rows. "
                    "If this drop is intentional, pass documented_filter= argument.",
                    section)

    return (n_before, n_after)

# COMMAND ----------

# =============================================================================
# 6. QUARANTINE HELPERS
# =============================================================================

_QUARANTINE_FRAMES = []

def quarantine(df, source_label: str, reason: str, section: str = "QUARANTINE"):
    """
    Tag unmatched / NEEDS_REVIEW rows and add to the quarantine accumulator.
    flush_quarantine() writes the combined report at end of validation run.
    """
    n = df.count()
    if n == 0:
        passed(f"Quarantine check '{source_label}': 0 rows — all matched.", section)
        return
    df_tagged = (df
                 .withColumn("_quarantine_source", F.lit(source_label))
                 .withColumn("_quarantine_reason", F.lit(reason))
                 .withColumn("_quarantine_ts",     F.lit(ts())))
    _QUARANTINE_FRAMES.append(df_tagged)
    warn(True, f"{n:,} rows quarantined from '{source_label}': {reason}", section)

def flush_quarantine():
    """Union all quarantine frames and dual-write to logs/phase3_quarantine_report.txt."""
    if not _QUARANTINE_FRAMES:
        log("INFO", "Quarantine is EMPTY — all rows mapped cleanly.", "QUARANTINE")
        return
    from functools import reduce
    df_all = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True),
                    _QUARANTINE_FRAMES)
    n_total = df_all.count()
    save_df(df_all, "phase3_quarantine_report.txt", "QUARANTINE")
    log("INFO",
        f"Quarantine report: {n_total:,} total quarantined rows "
        f"across {len(_QUARANTINE_FRAMES)} source(s).",
        "QUARANTINE")

# COMMAND ----------

# =============================================================================
# 7. FINAL GATE
# =============================================================================

def phase3_final_summary() -> str:
    """Print Phase 3 gate and return one of: CLEAR / CLEAR_WITH_WARNINGS / BLOCKED."""
    n_b = len(_HARD_BLOCKERS)
    n_w = len(_WARNINGS)

    log("INFO", "=" * 80, "FINAL SUMMARY")
    log("INFO", "PHASE 3 MDM STANDARDIZATION — FINAL STATUS REPORT", "FINAL SUMMARY")
    log("INFO", "=" * 80, "FINAL SUMMARY")

    if n_b > 0:
        log("🚨 BLOCKER", f"TOTAL HARD BLOCKERS: {n_b}", "FINAL SUMMARY")
        for i, b in enumerate(_HARD_BLOCKERS, 1):
            log("🚨 BLOCKER", f"  {i}. {b}", "FINAL SUMMARY")
        gate = "🔴 BLOCKED"
    else:
        log("✅ PASS", "NO HARD BLOCKERS — all structural assertions passed.", "FINAL SUMMARY")
        gate = "🟡 CLEAR WITH WARNINGS" if n_w > 0 else "🟢 CLEAR"

    if n_w > 0:
        log("⚠️  WARNING", f"TOTAL WARNINGS: {n_w}", "FINAL SUMMARY")
        for i, w in enumerate(_WARNINGS, 1):
            log("⚠️  WARNING", f"  {i}. {w}", "FINAL SUMMARY")

    log("INFO", f"PHASE 3 GATE: {gate}", "FINAL SUMMARY")

    if gate == "🟢 CLEAR":
        log("INFO",
            "Phase 3 standardization COMPLETE. All assertions PASS. "
            "Gold layer promotion approved.",
            "FINAL SUMMARY")
    elif gate == "🟡 CLEAR WITH WARNINGS":
        log("INFO",
            "Phase 3 COMPLETE WITH WARNINGS. Complete M1-M4 manual "
            "mappings and rerun before production gold promotion.",
            "FINAL SUMMARY")
    else:
        log("INFO",
            "Phase 3 BLOCKED. Resolve all BLOCKER items. "
            "Do NOT promote any *_std output to gold.",
            "FINAL SUMMARY")

    return gate

# COMMAND ----------

# =============================================================================
# 8. STARTUP BANNER
# =============================================================================

log("INFO", "=" * 70, "INIT")
log("INFO", "Phase 3 shared utilities — silver_homologation_apply.py loaded", "INIT")
log("INFO", f"REPO_ROOT     : {REPO_ROOT}", "INIT")
log("INFO", f"REPO_LOGS_DIR : {REPO_LOGS_DIR}", "INIT")
log("INFO", f"DBFS_ROOT     : {DBFS_ROOT}", "INIT")
log("INFO", f"ACTIVE_CATALOG_FILTER (R4, locked): '{ACTIVE_CATALOG_FILTER}'", "INIT")
log("INFO", "Credentials: PRD_MEX and PRD_MDP profiles ready.", "INIT")
log("INFO", "JOIN_REGISTRY initialized (call register_join() after every join).", "INIT")
log("INFO", "=" * 70, "INIT")
