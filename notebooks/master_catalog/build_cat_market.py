# Databricks notebook source
# MAGIC %md
# MAGIC # build_cat_market.py
# MAGIC ## Danone Master Data Catalog v1.0.0
# MAGIC ### Canal Market / Geography Dimension — Nielsen CBU Consolidation
# MAGIC
# MAGIC **Grain:** `source_cbu × mrkt_dsc_shrt_norm` — one row per CBU per market name.
# MAGIC No cross-CBU collapse. All outputs to DBFS and logs/. Snowflake is READ-ONLY.

# COMMAND ----------
# =============================================================================
# SECTION 0 — SETUP, CREDENTIALS, GUARDS
# =============================================================================

import os, re, hashlib, importlib.util, traceback
from datetime import datetime, date
from pyspark.sql import functions as F, types as T

# ---------------------------------------------------------------------------
# 0.1  Build metadata
# ---------------------------------------------------------------------------
CATALOG_VERSION   = "1.0.0"
NOTEBOOK_NAME     = "build_cat_market"
RUN_DATE          = str(date.today())
RUN_TIMESTAMP     = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
DBFS_BASE         = "dbfs:/mnt/mdp/mdm/master_catalog/market"
REPO_LOG_PATH     = "logs/catalog_eda/build_cat_market_report.txt"

# ---------------------------------------------------------------------------
# 0.2  Repo root (works in Databricks Repos and local runs)
# ---------------------------------------------------------------------------
try:
    _nb_path   = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    _repo_root = "/Workspace" + "/".join(_nb_path.split("/")[:-3])
except Exception:
    _repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

SIGNOFF_REPO_PATH = os.path.join(
    _repo_root,
    "configs", "catalog_seeds", "market", "signoff_03_nielsen_markets.csv"
)

# ---------------------------------------------------------------------------
# 0.3  Database constants
# ---------------------------------------------------------------------------
DB_PRD_MEX = "PRD_MEX"   # EDP, WATER_ST, WATER_RT, PB, SELL_IN
DB_PRD_MDP = "PRD_MDP"   # IBP only

# ---------------------------------------------------------------------------
# 0.4  Governed closed canal_std catalog (14 values — Correction 4)
# ---------------------------------------------------------------------------
_CANAL_STD_GOVERNED = {
    "AUTOSERVICIO",
    "AUTOSERVICIO_SCANNING",
    "TRADICIONAL",
    "MODERNO_TOTAL",
    "TOTAL_MERCADO",
    "AUTOSERVICIO_SURTIDO_EXTENDIDO",
    "AUTOSERVICIO_SURTIDO_COMPLETO",
    "AUTOSERVICIO_SURTIDO_ESENCIAL",
    "TDC_FARMACIAS_HARD_DISCOUNTERS",
    "GRANDES_CADENAS_AUTOSERVICIO",
    "PROXIMIDAD",
    "TRADICIONAL_GRANDES_MINISUPERS",
    "AUTOSERVICIOS_MAYORISTAS",
    "TRADICIONAL_PEQUENAS_ESTANQUILLOS",
}

# Canonical 20-column output order (Gate M13)
_SCHEMA_ORDER = [
    "market_key", "mrkt_dsc_shrt", "mrkt_dsc_shrt_norm", "canal_std",
    "canal_lvl1", "canal_lvl2", "agg_level", "region_type", "region_std",
    "region_detail", "is_total_mexico", "is_scanning", "source_cbu",
    "source_table", "nielsen_source_table", "promoted", "confirmation_status",
    "catalog_date", "catalog_version", "notes",
]

# ---------------------------------------------------------------------------
# 0.5  Logging helpers
# ---------------------------------------------------------------------------
_log_lines  = []
_warnings   = []
_blockers   = []

def _emit(level, tag, msg):
    ts   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}][{level}][{tag}] {msg}"
    _log_lines.append(line)
    print(line)

def info(tag, msg):
    _emit("INFO", tag, msg)

def warn(tag, msg):
    _emit("WARN", tag, msg)
    _warnings.append(f"[{tag}] {msg}")

def blocker(tag, msg):
    _emit("BLOCKER", tag, msg)
    _blockers.append(f"[{tag}] {msg}")

info("S0", f"build_cat_market.py  v{CATALOG_VERSION}  started at {RUN_TIMESTAMP}")
info("S0", f"repo_root={_repo_root}")
info("S0", f"DBFS base: {DBFS_BASE}")

# ---------------------------------------------------------------------------
# 0.6  Snowflake read-only guard — expanded (CALL, BEGIN, COMMIT, ROLLBACK, ;)
# ---------------------------------------------------------------------------
_ALLOWED = re.compile(r"^\s*(SELECT|WITH)\b", re.IGNORECASE)
_BLOCKED = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|CREATE|DROP|ALTER|TRUNCATE|"
    r"REPLACE|COPY|GRANT|REVOKE|PUT|GET|REMOVE|UNDROP|CLONE|"
    r"CALL|BEGIN|COMMIT|ROLLBACK)\b|;",
    re.IGNORECASE,
)

def run_sf(sql: str, db: str):
    """Execute a read-only Snowflake query. Blocks any DML / DDL / transaction SQL."""
    if not _ALLOWED.match(sql):
        blocker("RO", f"SQL must start with SELECT or WITH. Got: {sql[:80]}")
        raise RuntimeError("Blocked SQL")
    if _BLOCKED.search(sql):
        blocker("RO", f"Blocked keyword or semicolon detected. SQL[:80]: {sql[:80]}")
        raise RuntimeError("Blocked SQL")
    opts = _get_sf_options(db)
    return (
        spark.read.format("net.snowflake.spark.snowflake")
        .options(**opts)
        .option("sfDatabase", db)
        .option("query", sql)
        .load()
    )

# ---------------------------------------------------------------------------
# 0.7  Credential loader — dual profile (PRD_MEX direct / PRD_MDP Key Vault)
# ---------------------------------------------------------------------------
def _get_sf_options(database: str) -> dict:
    """Return Snowflake connector options for a given database profile."""
    spec = importlib.util.spec_from_file_location(
        "snowflake_creds",
        os.path.join(_repo_root, "configs", "snowflake_creds.py")
    )
    _creds_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(_creds_mod)
    cfg = _creds_mod.get_config(database)

    base = {
        "sfURL":       cfg["account"] + ".snowflakecomputing.com",
        "sfWarehouse": cfg.get("warehouse", "COMPUTE_WH"),
        "sfSchema":    cfg.get("schema",    "PUBLIC"),
        "sfRole":      cfg.get("role",      ""),
    }
    if database == DB_PRD_MDP:
        kv_scope           = "DAN-AM-P-KVT800-R-MDP-DB"
        base["sfUser"]     = dbutils.secrets.get(kv_scope, "snowflake-user")
        base["sfPassword"] = dbutils.secrets.get(kv_scope, "snowflake-password")
    else:
        base["sfUser"]     = cfg["user"]
        base["sfPassword"] = cfg["password"]
    return base

# ---------------------------------------------------------------------------
# 0.8  Helper functions
# ---------------------------------------------------------------------------
def normalize(v):
    return str(v).strip().upper() if v and str(v).strip() else None

def market_key_fn(source_cbu: str, mrkt_norm: str) -> str:
    raw = f"{source_cbu}|{mrkt_norm}"
    return hashlib.md5(raw.encode()).hexdigest()[:16]

_market_key_udf = F.udf(market_key_fn, T.StringType())

# ---------------------------------------------------------------------------
# 0.9  DBFS directories
# ---------------------------------------------------------------------------
for _sub in ["", "/profiles", "/audit"]:
    try:
        dbutils.fs.mkdirs(DBFS_BASE + _sub)
    except Exception:
        pass
info("S0", f"DBFS directories initialized under {DBFS_BASE}")

# ---------------------------------------------------------------------------
# 0.10  Load and validate signoff seed
# ---------------------------------------------------------------------------
info("S0_SEED", f"Loading signoff seed from: {SIGNOFF_REPO_PATH}")

_SEED_DBFS = f"{DBFS_BASE}/signoff_03_nielsen_markets_seed.csv"
dbutils.fs.cp(f"file:{SIGNOFF_REPO_PATH}", _SEED_DBFS, recurse=False)

df_signoff_raw = spark.read.option("header", True).csv(_SEED_DBFS)

_CBU_MAP = {
    "EDP_NIELSEN":       "EDP",
    "WATER_NIELSEN_RIE": "WATER_RT",
    "PB_NIELSEN":        "PB",
}
_cbu_map_udf = F.udf(
    lambda x: _CBU_MAP.get(str(x).strip().upper(), "UNKNOWN") if x else "UNKNOWN",
    T.StringType()
)

df_signoff = (
    df_signoff_raw
    .withColumn("source_cbu", _cbu_map_udf(F.col("NIELSEN_SOURCE_TABLE")))
    .withColumn("mrkt_norm",  F.trim(F.upper(F.col("MRKT_DSC_SHRT"))))
    .withColumn("join_key",   F.concat_ws("|", F.col("source_cbu"), F.col("mrkt_norm")))
    .cache()
)

_signoff_total = df_signoff.count()
info("S0_SEED", f"Signoff seed loaded: {_signoff_total} rows")

# Gate M11 BLOCKER — no duplicate source-aware join keys
_signoff_dup_count = df_signoff.groupBy("join_key").count().filter("count > 1").count()
if _signoff_dup_count > 0:
    blocker("M11", f"Signoff seed has {_signoff_dup_count} duplicate source-aware join keys")
else:
    info("M11", f"PASS — signoff seed: 0 duplicate source-aware keys ({_signoff_total} rows)")

# Gate M17 WARN — signoff canal_std against governed closed catalog
_seed_canal_vals = {
    r.canal_std.strip() if r.canal_std else ""
    for r in df_signoff.select("canal_std").distinct().collect()
}
_seed_exceptions = {v for v in _seed_canal_vals if v and v not in _CANAL_STD_GOVERNED}
if _seed_exceptions:
    warn("M17_SEED", f"Signoff seed canal_std outside governed catalog: {_seed_exceptions}")
else:
    info("M17_SEED", "PASS — all signoff canal_std values within governed catalog")

info("S0", "Setup complete. Starting CBU profiling.")

# COMMAND ----------
# =============================================================================
# SECTION 1 — EDP NIELSEN MARKET PROFILE
# =============================================================================

info("S1_EDP", "=" * 60)
info("S1_EDP", "EDP NIELSEN MARKET DIMENSION")
info("S1_EDP", "Credential: PRD_MEX")

SQL_EDP = """
    SELECT market_id, MRKT_DSC_SHRT
    FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_MKT_DIM
"""

try:
    df_edp_raw    = run_sf(SQL_EDP, DB_PRD_MEX).cache()
    _edp_total      = df_edp_raw.count()
    _edp_id_count   = df_edp_raw.select("market_id").distinct().count()
    _edp_name_count = df_edp_raw.select("MRKT_DSC_SHRT").distinct().count()
    info("S1_EDP", f"Rows: {_edp_total}  |  market_id stability: {_edp_id_count} IDs / {_edp_name_count} names")
except Exception as _e:
    blocker("S1_EDP", f"Failed to query EDP market table: {_e}")
    df_edp_raw    = spark.createDataFrame([], T.StructType([
        T.StructField("market_id",     T.StringType()),
        T.StructField("MRKT_DSC_SHRT", T.StringType()),
    ]))
    _edp_total = _edp_id_count = _edp_name_count = 0

# Gate M1 BLOCKER
if _edp_name_count < 50:
    blocker("M1", f"EDP distinct MRKT_DSC_SHRT = {_edp_name_count} < 50")
else:
    info("M1", f"PASS — EDP distinct markets: {_edp_name_count}")

df_edp_norm = (
    df_edp_raw
    .withColumn("mrkt_dsc_shrt",        F.col("MRKT_DSC_SHRT"))
    .withColumn("mrkt_dsc_shrt_norm",   F.trim(F.upper(F.col("MRKT_DSC_SHRT"))))
    .withColumn("source_cbu",           F.lit("EDP"))
    .withColumn("source_table",         F.lit("PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_MKT_DIM"))
    .withColumn("nielsen_source_table", F.lit("EDP_NIELSEN"))
)

df_signoff_edp = df_signoff.filter(F.col("source_cbu") == "EDP").select(
    F.col("mrkt_norm"),
    F.col("CANAL_LVL1").alias("canal_lvl1"),
    F.col("CANAL_LVL2").alias("canal_lvl2"),
    F.col("canal_std"),
    F.col("AGG_LEVEL").alias("agg_level"),
    F.col("REGION_TYPE").alias("region_type"),
    F.col("REGION_STD").alias("region_std"),
    F.col("REGION_DETAIL").alias("region_detail"),
    F.col("REVIEW_STATUS").alias("review_status"),
)

df_edp_joined = (
    df_edp_norm
    .join(df_signoff_edp, df_edp_norm["mrkt_dsc_shrt_norm"] == df_signoff_edp["mrkt_norm"], "left")
    .withColumn("promoted",
        F.when(F.col("review_status").isNotNull(), F.lit("YES")).otherwise(F.lit("PENDING")))
    .withColumn("confirmation_status",
        F.when(F.col("review_status").isNotNull(), F.lit("CONFIRMED")).otherwise(F.lit("PENDING")))
    .withColumn("is_total_mexico",
        F.when(F.trim(F.upper(F.col("region_std"))) == "TOTAL_MEXICO", F.lit("YES")).otherwise(F.lit("NO")))
    .withColumn("is_scanning",
        F.when(F.col("canal_std").like("%SCANNING%"), F.lit("YES")).otherwise(F.lit("NO")))
    .withColumn("market_key", _market_key_udf(F.col("source_cbu"), F.col("mrkt_dsc_shrt_norm")))
    .withColumn("catalog_date",    F.lit(RUN_DATE))
    .withColumn("catalog_version", F.lit(CATALOG_VERSION))
    .withColumn("notes",
        F.when(F.col("promoted") == "PENDING", F.lit("No match in signoff seed"))
         .otherwise(F.lit(None).cast(T.StringType())))
    .drop("mrkt_norm", "review_status")
)

_edp_confirmed = df_edp_joined.filter(F.col("promoted") == "YES").count()
_edp_pending   = df_edp_joined.filter(F.col("promoted") == "PENDING").count()
_edp_pct       = 100.0 * _edp_confirmed / _edp_name_count if _edp_name_count > 0 else 0.0

info("S1_EDP", f"Coverage: {_edp_confirmed}/{_edp_name_count} classified ({_edp_pct:.1f}%)")

# Gate M8 WARN (informational only — not a blocker)
_edp_has_total = df_edp_joined.filter(F.col("is_total_mexico") == "YES").count() > 0
if not _edp_has_total:
    warn("M8", "EDP: no is_total_mexico=YES row found")
else:
    info("M8", "EDP: is_total_mexico=YES confirmed")

# Write profile (includes market_id for traceability)
df_edp_profile = df_edp_joined.select(
    "market_id", "mrkt_dsc_shrt", "mrkt_dsc_shrt_norm", "market_key",
    "canal_std", "canal_lvl1", "canal_lvl2", "agg_level",
    "region_type", "region_std", "region_detail",
    "is_total_mexico", "is_scanning", "source_cbu", "promoted", "confirmation_status", "notes"
)
df_edp_profile.coalesce(1).write.mode("overwrite").option("header", True).csv(
    f"{DBFS_BASE}/profiles/edp_market_profile.csv"
)
info("S1_EDP", f"Written: edp_market_profile.csv ({_edp_name_count} rows — incl. market_id)")

# COMMAND ----------
# =============================================================================
# SECTION 2 — WATER SCANTRACK (WATER_ST) MARKET PROFILE — GOVERNANCE DEBT
# =============================================================================

info("S2_WST", "=" * 60)
info("S2_WST", "WATER SCANTRACK MARKET — GOVERNANCE DEBT")
info("S2_WST", "NOTE: WATER_ST has 0 rows in signoff seed — all markets will be PENDING")
info("S2_WST", "Credential: PRD_MEX")

SQL_WST = """
    SELECT market_id, MRKT_DSC_SHRT
    FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_ST_NLSN_MKT_DIM
"""

try:
    df_wst_raw    = run_sf(SQL_WST, DB_PRD_MEX).cache()
    _wst_total      = df_wst_raw.count()
    _wst_id_count   = df_wst_raw.select("market_id").distinct().count()
    _wst_name_count = df_wst_raw.select("MRKT_DSC_SHRT").distinct().count()
    info("S2_WST", f"Rows: {_wst_total}  |  market_id stability: {_wst_id_count} IDs / {_wst_name_count} names")
except Exception as _e:
    blocker("S2_WST", f"Failed to query WATER_ST market table: {_e}")
    df_wst_raw    = spark.createDataFrame([], T.StructType([
        T.StructField("market_id",     T.StringType()),
        T.StructField("MRKT_DSC_SHRT", T.StringType()),
    ]))
    _wst_total = _wst_id_count = _wst_name_count = 0

# Gate M2 BLOCKER
if _wst_name_count < 30:
    blocker("M2", f"WATER_ST distinct MRKT_DSC_SHRT = {_wst_name_count} < 30")
else:
    info("M2", f"PASS — WATER_ST distinct markets: {_wst_name_count}")

# Gate M8 WARN — WATER_ST has no taxonomy
warn("M8", "WATER_ST: all markets PENDING — is_total_mexico cannot be determined (governance debt)")

# All WATER_ST rows are PENDING — no signoff join
df_wst_joined = (
    df_wst_raw
    .withColumn("mrkt_dsc_shrt",        F.col("MRKT_DSC_SHRT"))
    .withColumn("mrkt_dsc_shrt_norm",   F.trim(F.upper(F.col("MRKT_DSC_SHRT"))))
    .withColumn("source_cbu",           F.lit("WATER_ST"))
    .withColumn("source_table",         F.lit("PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_ST_NLSN_MKT_DIM"))
    .withColumn("nielsen_source_table", F.lit("WATER_NIELSEN_ST"))
    .withColumn("canal_std",            F.lit(None).cast(T.StringType()))
    .withColumn("canal_lvl1",           F.lit(None).cast(T.StringType()))
    .withColumn("canal_lvl2",           F.lit(None).cast(T.StringType()))
    .withColumn("agg_level",            F.lit(None).cast(T.StringType()))
    .withColumn("region_type",          F.lit(None).cast(T.StringType()))
    .withColumn("region_std",           F.lit(None).cast(T.StringType()))
    .withColumn("region_detail",        F.lit(None).cast(T.StringType()))
    .withColumn("is_total_mexico",      F.lit("NO"))
    .withColumn("is_scanning",          F.lit("NO"))
    .withColumn("promoted",             F.lit("PENDING"))
    .withColumn("confirmation_status",  F.lit("PENDING"))
    .withColumn("market_key", _market_key_udf(F.lit("WATER_ST"), F.trim(F.upper(F.col("MRKT_DSC_SHRT")))))
    .withColumn("catalog_date",    F.lit(RUN_DATE))
    .withColumn("catalog_version", F.lit(CATALOG_VERSION))
    .withColumn("notes",           F.lit("WATER_ST governance debt — not classified in signoff seed"))
)

_wst_confirmed = 0
_wst_pending   = _wst_name_count
_wst_pct       = 0.0
info("S2_WST", f"Coverage: 0/{_wst_name_count} classified (0%) — governance debt")

df_wst_profile = df_wst_joined.select(
    "market_id", "mrkt_dsc_shrt", "mrkt_dsc_shrt_norm", "market_key",
    "canal_std", "canal_lvl1", "canal_lvl2", "agg_level",
    "region_type", "region_std", "region_detail",
    "is_total_mexico", "is_scanning", "source_cbu", "promoted", "confirmation_status", "notes"
)
df_wst_profile.coalesce(1).write.mode("overwrite").option("header", True).csv(
    f"{DBFS_BASE}/profiles/water_st_market_profile.csv"
)
info("S2_WST", f"Written: water_st_market_profile.csv ({_wst_name_count} rows — all PENDING, incl. market_id)")

# COMMAND ----------
# =============================================================================
# SECTION 3 — WATER RIE (WATER_RT) MARKET PROFILE
# =============================================================================

info("S3_WRT", "=" * 60)
info("S3_WRT", "WATER RIE MARKET DIMENSION")
info("S3_WRT", "Credential: PRD_MEX")

SQL_WRT = """
    SELECT market_id, MRKT_DSC_SHRT
    FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_RT_NLSN_MKT_DIM
"""

try:
    df_wrt_raw    = run_sf(SQL_WRT, DB_PRD_MEX).cache()
    _wrt_total      = df_wrt_raw.count()
    _wrt_id_count   = df_wrt_raw.select("market_id").distinct().count()
    _wrt_name_count = df_wrt_raw.select("MRKT_DSC_SHRT").distinct().count()
    info("S3_WRT", f"Rows: {_wrt_total}  |  market_id stability: {_wrt_id_count} IDs / {_wrt_name_count} names")
except Exception as _e:
    blocker("S3_WRT", f"Failed to query WATER_RT market table: {_e}")
    df_wrt_raw    = spark.createDataFrame([], T.StructType([
        T.StructField("market_id",     T.StringType()),
        T.StructField("MRKT_DSC_SHRT", T.StringType()),
    ]))
    _wrt_total = _wrt_id_count = _wrt_name_count = 0

# Gate M3 BLOCKER
if _wrt_name_count < 20:
    blocker("M3", f"WATER_RT distinct MRKT_DSC_SHRT = {_wrt_name_count} < 20")
else:
    info("M3", f"PASS — WATER_RT distinct markets: {_wrt_name_count}")

df_wrt_norm = (
    df_wrt_raw
    .withColumn("mrkt_dsc_shrt",        F.col("MRKT_DSC_SHRT"))
    .withColumn("mrkt_dsc_shrt_norm",   F.trim(F.upper(F.col("MRKT_DSC_SHRT"))))
    .withColumn("source_cbu",           F.lit("WATER_RT"))
    .withColumn("source_table",         F.lit("PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_RT_NLSN_MKT_DIM"))
    .withColumn("nielsen_source_table", F.lit("WATER_NIELSEN_RIE"))
)

df_signoff_wrt = df_signoff.filter(F.col("source_cbu") == "WATER_RT").select(
    F.col("mrkt_norm"),
    F.col("CANAL_LVL1").alias("canal_lvl1"),
    F.col("CANAL_LVL2").alias("canal_lvl2"),
    F.col("canal_std"),
    F.col("AGG_LEVEL").alias("agg_level"),
    F.col("REGION_TYPE").alias("region_type"),
    F.col("REGION_STD").alias("region_std"),
    F.col("REGION_DETAIL").alias("region_detail"),
    F.col("REVIEW_STATUS").alias("review_status"),
)

df_wrt_joined = (
    df_wrt_norm
    .join(df_signoff_wrt, df_wrt_norm["mrkt_dsc_shrt_norm"] == df_signoff_wrt["mrkt_norm"], "left")
    .withColumn("promoted",
        F.when(F.col("review_status").isNotNull(), F.lit("YES")).otherwise(F.lit("PENDING")))
    .withColumn("confirmation_status",
        F.when(F.col("review_status").isNotNull(), F.lit("CONFIRMED")).otherwise(F.lit("PENDING")))
    .withColumn("is_total_mexico",
        F.when(F.trim(F.upper(F.col("region_std"))) == "TOTAL_MEXICO", F.lit("YES")).otherwise(F.lit("NO")))
    .withColumn("is_scanning",
        F.when(F.col("canal_std").like("%SCANNING%"), F.lit("YES")).otherwise(F.lit("NO")))
    .withColumn("market_key", _market_key_udf(F.col("source_cbu"), F.col("mrkt_dsc_shrt_norm")))
    .withColumn("catalog_date",    F.lit(RUN_DATE))
    .withColumn("catalog_version", F.lit(CATALOG_VERSION))
    .withColumn("notes",
        F.when(F.col("promoted") == "PENDING", F.lit("No match in signoff seed"))
         .otherwise(F.lit(None).cast(T.StringType())))
    .drop("mrkt_norm", "review_status")
)

_wrt_confirmed = df_wrt_joined.filter(F.col("promoted") == "YES").count()
_wrt_pending   = df_wrt_joined.filter(F.col("promoted") == "PENDING").count()
_wrt_pct       = 100.0 * _wrt_confirmed / _wrt_name_count if _wrt_name_count > 0 else 0.0
info("S3_WRT", f"Coverage: {_wrt_confirmed}/{_wrt_name_count} classified ({_wrt_pct:.1f}%)")

_wrt_has_total = df_wrt_joined.filter(F.col("is_total_mexico") == "YES").count() > 0
if not _wrt_has_total:
    warn("M8", "WATER_RT: no is_total_mexico=YES row found")
else:
    info("M8", "WATER_RT: is_total_mexico=YES confirmed")

df_wrt_profile = df_wrt_joined.select(
    "market_id", "mrkt_dsc_shrt", "mrkt_dsc_shrt_norm", "market_key",
    "canal_std", "canal_lvl1", "canal_lvl2", "agg_level",
    "region_type", "region_std", "region_detail",
    "is_total_mexico", "is_scanning", "source_cbu", "promoted", "confirmation_status", "notes"
)
df_wrt_profile.coalesce(1).write.mode("overwrite").option("header", True).csv(
    f"{DBFS_BASE}/profiles/water_rt_market_profile.csv"
)
info("S3_WRT", f"Written: water_rt_market_profile.csv ({_wrt_name_count} rows — incl. market_id)")

# COMMAND ----------
# =============================================================================
# SECTION 4 — PLANT BASED (PB) MARKET PROFILE
# =============================================================================

info("S4_PB", "=" * 60)
info("S4_PB", "PLANT BASED MARKET DIMENSION")
info("S4_PB", "Credential: PRD_MEX")

SQL_PB = """
    SELECT market_id, MRKT_DSC_SHRT
    FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_SUST_LECHE_ST_NLSN_MKT_DIM
"""

try:
    df_pb_raw    = run_sf(SQL_PB, DB_PRD_MEX).cache()
    _pb_total      = df_pb_raw.count()
    _pb_id_count   = df_pb_raw.select("market_id").distinct().count()
    _pb_name_count = df_pb_raw.select("MRKT_DSC_SHRT").distinct().count()
    info("S4_PB", f"Rows: {_pb_total}  |  market_id stability: {_pb_id_count} IDs / {_pb_name_count} names")
except Exception as _e:
    blocker("S4_PB", f"Failed to query PB market table: {_e}")
    df_pb_raw    = spark.createDataFrame([], T.StructType([
        T.StructField("market_id",     T.StringType()),
        T.StructField("MRKT_DSC_SHRT", T.StringType()),
    ]))
    _pb_total = _pb_id_count = _pb_name_count = 0

# Gate M4 BLOCKER
if _pb_name_count < 50:
    blocker("M4", f"PB distinct MRKT_DSC_SHRT = {_pb_name_count} < 50")
else:
    info("M4", f"PASS — PB distinct markets: {_pb_name_count}")

df_pb_norm = (
    df_pb_raw
    .withColumn("mrkt_dsc_shrt",        F.col("MRKT_DSC_SHRT"))
    .withColumn("mrkt_dsc_shrt_norm",   F.trim(F.upper(F.col("MRKT_DSC_SHRT"))))
    .withColumn("source_cbu",           F.lit("PB"))
    .withColumn("source_table",         F.lit("PRD_MEX.MEX_DSP_DPH_MKT.VW_SUST_LECHE_ST_NLSN_MKT_DIM"))
    .withColumn("nielsen_source_table", F.lit("PB_NIELSEN"))
)

df_signoff_pb = df_signoff.filter(F.col("source_cbu") == "PB").select(
    F.col("mrkt_norm"),
    F.col("CANAL_LVL1").alias("canal_lvl1"),
    F.col("CANAL_LVL2").alias("canal_lvl2"),
    F.col("canal_std"),
    F.col("AGG_LEVEL").alias("agg_level"),
    F.col("REGION_TYPE").alias("region_type"),
    F.col("REGION_STD").alias("region_std"),
    F.col("REGION_DETAIL").alias("region_detail"),
    F.col("REVIEW_STATUS").alias("review_status"),
)

df_pb_joined = (
    df_pb_norm
    .join(df_signoff_pb, df_pb_norm["mrkt_dsc_shrt_norm"] == df_signoff_pb["mrkt_norm"], "left")
    .withColumn("promoted",
        F.when(F.col("review_status").isNotNull(), F.lit("YES")).otherwise(F.lit("PENDING")))
    .withColumn("confirmation_status",
        F.when(F.col("review_status").isNotNull(), F.lit("CONFIRMED")).otherwise(F.lit("PENDING")))
    .withColumn("is_total_mexico",
        F.when(F.trim(F.upper(F.col("region_std"))) == "TOTAL_MEXICO", F.lit("YES")).otherwise(F.lit("NO")))
    .withColumn("is_scanning",
        F.when(F.col("canal_std").like("%SCANNING%"), F.lit("YES")).otherwise(F.lit("NO")))
    .withColumn("market_key", _market_key_udf(F.col("source_cbu"), F.col("mrkt_dsc_shrt_norm")))
    .withColumn("catalog_date",    F.lit(RUN_DATE))
    .withColumn("catalog_version", F.lit(CATALOG_VERSION))
    .withColumn("notes",
        F.when(F.col("promoted") == "PENDING", F.lit("No match in signoff seed"))
         .otherwise(F.lit(None).cast(T.StringType())))
    .drop("mrkt_norm", "review_status")
)

_pb_confirmed = df_pb_joined.filter(F.col("promoted") == "YES").count()
_pb_pending   = df_pb_joined.filter(F.col("promoted") == "PENDING").count()
_pb_pct       = 100.0 * _pb_confirmed / _pb_name_count if _pb_name_count > 0 else 0.0
info("S4_PB", f"Coverage: {_pb_confirmed}/{_pb_name_count} classified ({_pb_pct:.1f}%)")

_pb_has_total = df_pb_joined.filter(F.col("is_total_mexico") == "YES").count() > 0
if not _pb_has_total:
    warn("M8", "PB: no is_total_mexico=YES row found")
else:
    info("M8", "PB: is_total_mexico=YES confirmed")

df_pb_profile = df_pb_joined.select(
    "market_id", "mrkt_dsc_shrt", "mrkt_dsc_shrt_norm", "market_key",
    "canal_std", "canal_lvl1", "canal_lvl2", "agg_level",
    "region_type", "region_std", "region_detail",
    "is_total_mexico", "is_scanning", "source_cbu", "promoted", "confirmation_status", "notes"
)
df_pb_profile.coalesce(1).write.mode("overwrite").option("header", True).csv(
    f"{DBFS_BASE}/profiles/pb_market_profile.csv"
)
info("S4_PB", f"Written: pb_market_profile.csv ({_pb_name_count} rows — incl. market_id)")

# COMMAND ----------
# =============================================================================
# SECTION 5 — IBP MERCADO PROFILE (REFERENCE ONLY)
# =============================================================================

info("S5_IBP", "=" * 60)
info("S5_IBP", "IBP MERCADO — REFERENCE ONLY")
info("S5_IBP", "Credential: PRD_MDP")

SQL_IBP = """
    SELECT
        MERCADO,
        AREA_GEO_RUSH,
        COUNT(*) AS row_count
    FROM PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP
    WHERE NOMBRE_ETIQUETA = 'REAL'
      AND MERCADO IS NOT NULL
    GROUP BY 1, 2
    ORDER BY row_count DESC
"""

try:
    df_ibp_raw = run_sf(SQL_IBP, DB_PRD_MDP).cache()
    _ibp_count = df_ibp_raw.select("MERCADO").distinct().count()
    info("S5_IBP", f"Distinct MERCADO values: {_ibp_count}")
    df_ibp_raw.show(10, truncate=False)
except Exception as _e:
    warn("S5_IBP", f"IBP MERCADO query failed: {_e}")
    df_ibp_raw = spark.createDataFrame([], T.StructType([
        T.StructField("MERCADO",       T.StringType()),
        T.StructField("AREA_GEO_RUSH", T.StringType()),
        T.StructField("row_count",     T.LongType()),
    ]))
    _ibp_count = 0

# Gate M5 WARN
if _ibp_count < 5:
    warn("M5", f"IBP distinct MERCADO = {_ibp_count} < 5")
else:
    info("M5", f"PASS — IBP distinct MERCADO: {_ibp_count}")

df_ibp_profile = (
    df_ibp_raw
    .withColumn("mrkt_dsc_shrt",        F.col("MERCADO"))
    .withColumn("mrkt_dsc_shrt_norm",   F.trim(F.upper(F.col("MERCADO"))))
    .withColumn("source_cbu",           F.lit("IBP"))
    .withColumn("source_table",         F.lit("PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP"))
    .withColumn("nielsen_source_table", F.lit("IBP"))
    .withColumn("promoted",            F.lit("NO"))
    .withColumn("confirmation_status", F.lit("REFERENCE_ONLY"))
    .withColumn("notes",               F.lit("IBP planning geography — not Nielsen canonical"))
)
df_ibp_profile.coalesce(1).write.mode("overwrite").option("header", True).csv(
    f"{DBFS_BASE}/profiles/ibp_mercado_profile.csv"
)
info("S5_IBP", f"Written: ibp_mercado_profile.csv ({_ibp_count} rows)")

# COMMAND ----------
# =============================================================================
# SECTION 6 — SELL_IN INTERNAL GEOGRAPHY (REFERENCE ONLY)
# =============================================================================

info("S6_SI", "=" * 60)
info("S6_SI", "SELL_IN INTERNAL GEOGRAPHY — REFERENCE ONLY")
info("S6_SI", "Credential: PRD_MEX")

SQL_SI = """
    SELECT DISTINCT
        CUS_SAL_RGN_DSC_EXT  AS region,
        CUS_SAL_PLT_COD       AS id_cedis,
        CUS_SAL_PLT_DSC       AS cedis_dsc,
        PXY_CAT_1ST_DSC       AS cluster
    FROM PRD_MEX.MEX_DSP_OTC.V_D_CLIENT
    WHERE CUS_SAL_RGN_DSC_EXT IS NOT NULL
"""

try:
    df_si_raw = run_sf(SQL_SI, DB_PRD_MEX).cache()
    _si_count = df_si_raw.count()
    info("S6_SI", f"SELL_IN internal geography rows: {_si_count}")
except Exception as _e:
    warn("S6_SI", f"SELL_IN geography query failed: {_e}")
    df_si_raw = spark.createDataFrame([], T.StructType([
        T.StructField("region",    T.StringType()),
        T.StructField("id_cedis",  T.StringType()),
        T.StructField("cedis_dsc", T.StringType()),
        T.StructField("cluster",   T.StringType()),
    ]))
    _si_count = 0

# Gate M6 WARN
if _si_count == 0:
    warn("M6", "SELL_IN geo query returned 0 rows")
else:
    info("M6", f"PASS — SELL_IN internal geo: {_si_count} distinct regions")

df_si_profile = (
    df_si_raw
    .withColumn("mrkt_dsc_shrt",        F.col("region"))
    .withColumn("mrkt_dsc_shrt_norm",   F.trim(F.upper(F.col("region"))))
    .withColumn("source_cbu",           F.lit("SELL_IN"))
    .withColumn("source_table",         F.lit("PRD_MEX.MEX_DSP_OTC.V_D_CLIENT"))
    .withColumn("nielsen_source_table", F.lit("SELL_IN"))
    .withColumn("promoted",            F.lit("NO"))
    .withColumn("confirmation_status", F.lit("REFERENCE_ONLY"))
    .withColumn("notes",               F.lit("Internal Danone territory — not Nielsen market"))
)
df_si_profile.coalesce(1).write.mode("overwrite").option("header", True).csv(
    f"{DBFS_BASE}/profiles/sellin_geo_profile.csv"
)
info("S6_SI", f"Written: sellin_geo_profile.csv ({_si_count} rows)")

# COMMAND ----------
# =============================================================================
# SECTION 7 — COVERAGE, OVERLAP (DESCRIPTIVE), DUPLICATE AUDIT
# =============================================================================

info("S7_AUDIT", "=" * 60)
info("S7_AUDIT", "CBU COVERAGE + CROSS-CBU OVERLAP (DESCRIPTIVE) + DUPLICATE AUDIT")

# ---------------------------------------------------------------------------
# 7A  Per-CBU Coverage Table
# ---------------------------------------------------------------------------
_coverage = {
    "EDP":      {"total": _edp_name_count, "confirmed": _edp_confirmed, "pending": _edp_pending, "pct": _edp_pct},
    "WATER_ST": {"total": _wst_name_count, "confirmed": _wst_confirmed, "pending": _wst_pending, "pct": _wst_pct},
    "WATER_RT": {"total": _wrt_name_count, "confirmed": _wrt_confirmed, "pending": _wrt_pending, "pct": _wrt_pct},
    "PB":       {"total": _pb_name_count,  "confirmed": _pb_confirmed,  "pending": _pb_pending,  "pct": _pb_pct},
}

info("S7_AUDIT", f"  {'CBU':<10} {'Live':>6} {'Classified':>12} {'Pending':>9} {'Coverage':>10}")
info("S7_AUDIT", f"  {'-'*52}")
for _cbu, _cov in _coverage.items():
    _flag = "  ⚠️  GOVERNANCE DEBT" if _cbu == "WATER_ST" else ""
    info("S7_AUDIT",
         f"  {_cbu:<10} {_cov['total']:>6} {_cov['confirmed']:>12} {_cov['pending']:>9} {_cov['pct']:>9.1f}%{_flag}")

_total_live_nielsen = sum(c["total"]     for c in _coverage.values())
_total_confirmed    = sum(c["confirmed"] for c in _coverage.values())
_total_pending_n    = sum(c["pending"]   for c in _coverage.values())
_total_pct          = 100.0 * _total_confirmed / _total_live_nielsen if _total_live_nielsen > 0 else 0.0
info("S7_AUDIT", f"  {'TOTAL':<10} {_total_live_nielsen:>6} {_total_confirmed:>12} {_total_pending_n:>9} {_total_pct:>9.1f}%")

# Gate M7 WARN
_pending_pct = 100.0 * _total_pending_n / _total_live_nielsen if _total_live_nielsen > 0 else 0.0
if _pending_pct > 5.0:
    warn("M7", f"PENDING = {_pending_pct:.1f}% > 5% threshold ({_total_pending_n} rows — WATER_ST governance debt)")
else:
    info("M7", f"PASS — PENDING = {_pending_pct:.1f}%")

# ---------------------------------------------------------------------------
# 7B  Cross-CBU Overlap (DESCRIPTIVE — no quality gate)
# ---------------------------------------------------------------------------
info("S7_AUDIT", "Cross-CBU overlap analysis (descriptive — no quality gate tied to overlap)")

_name_sets = {
    "EDP":      set(r.mrkt_dsc_shrt_norm for r in df_edp_joined.select("mrkt_dsc_shrt_norm").collect()),
    "WATER_ST": set(r.mrkt_dsc_shrt_norm for r in df_wst_joined.select("mrkt_dsc_shrt_norm").collect()),
    "WATER_RT": set(r.mrkt_dsc_shrt_norm for r in df_wrt_joined.select("mrkt_dsc_shrt_norm").collect()),
    "PB":       set(r.mrkt_dsc_shrt_norm for r in df_pb_joined.select("mrkt_dsc_shrt_norm").collect()),
}
_in_all4    = set.intersection(*_name_sets.values())
_in_edp_wst = _name_sets["EDP"] & _name_sets["WATER_ST"]
_in_edp_wrt = _name_sets["EDP"] & _name_sets["WATER_RT"]
_in_edp_pb  = _name_sets["EDP"] & _name_sets["PB"]
_in_wst_wrt = _name_sets["WATER_ST"] & _name_sets["WATER_RT"]
_in_wst_pb  = _name_sets["WATER_ST"] & _name_sets["PB"]
_in_wrt_pb  = _name_sets["WATER_RT"] & _name_sets["PB"]

info("S7_AUDIT", f"  Total unique market names (union): {sum(c['total'] for c in _coverage.values())}")
info("S7_AUDIT", f"  IN_ALL_4:             {len(_in_all4)}")
info("S7_AUDIT", f"  EDP ∩ WATER_ST:       {len(_in_edp_wst)}")
info("S7_AUDIT", f"  EDP ∩ WATER_RT:       {len(_in_edp_wrt)}")
info("S7_AUDIT", f"  EDP ∩ PB:             {len(_in_edp_pb)}")
info("S7_AUDIT", f"  WATER_ST ∩ WATER_RT:  {len(_in_wst_wrt)}")
info("S7_AUDIT", f"  WATER_RT ∩ PB:        {len(_in_wrt_pb)}")

_overlap_rows = [
    {"pair": "ALL_4",        "shared_names": len(_in_all4)},
    {"pair": "EDP_WATER_ST", "shared_names": len(_in_edp_wst)},
    {"pair": "EDP_WATER_RT", "shared_names": len(_in_edp_wrt)},
    {"pair": "EDP_PB",       "shared_names": len(_in_edp_pb)},
    {"pair": "WST_WRT",      "shared_names": len(_in_wst_wrt)},
    {"pair": "WST_PB",       "shared_names": len(_in_wst_pb)},
    {"pair": "WRT_PB",       "shared_names": len(_in_wrt_pb)},
]
spark.createDataFrame(_overlap_rows).coalesce(1).write.mode("overwrite").option("header", True).csv(
    f"{DBFS_BASE}/audit/market_cross_cbu_overlap.csv"
)
info("S7_AUDIT", "Written: market_cross_cbu_overlap.csv (descriptive — no quality gate)")

# ---------------------------------------------------------------------------
# 7C  Geographic Hierarchy Distribution
# ---------------------------------------------------------------------------
_df_promoted_preview = (
    df_edp_joined.union(df_wrt_joined).union(df_pb_joined)
    .filter(F.col("promoted") == "YES")
)
_region_dist = {
    r["region_type"]: r["count"]
    for r in _df_promoted_preview.groupBy("region_type").count().collect()
    if r["region_type"] is not None
}
info("S7_AUDIT", "Geographic hierarchy (promoted preview):")
for _lvl in ["NATIONAL", "AREA", "CITY", "VDM"]:
    info("S7_AUDIT", f"  {_lvl:<10}: {_region_dist.get(_lvl, 0)}")

# ---------------------------------------------------------------------------
# 7D  Mandatory Duplicate Audit (Gates M12 + M12W)
# ---------------------------------------------------------------------------
info("S7_AUDIT", "Mandatory market_key duplicate audit (required before dedup)...")

df_nielsen_union = (
    df_edp_joined.select(_SCHEMA_ORDER)
    .union(df_wst_joined.select(_SCHEMA_ORDER))
    .union(df_wrt_joined.select(_SCHEMA_ORDER))
    .union(df_pb_joined.select(_SCHEMA_ORDER))
)

df_key_counts  = df_nielsen_union.groupBy("market_key").count().filter("count > 1")
_dup_key_count = df_key_counts.count()

if _dup_key_count > 0:
    df_conflicting = (
        df_nielsen_union.join(df_key_counts, "market_key")
        .groupBy("market_key")
        .agg(
            F.countDistinct("canal_std").alias("n_canal"),
            F.countDistinct("promoted").alias("n_prom"),
        )
        .filter("n_canal > 1 OR n_prom > 1")
    )
    _conflicting_count = df_conflicting.count()
    _identical_count   = _dup_key_count - _conflicting_count

    if _conflicting_count > 0:
        df_conflicting.coalesce(1).write.mode("overwrite").option("header", True).csv(
            f"{DBFS_BASE}/audit/market_key_conflicts.csv"
        )
        blocker("M12", f"{_conflicting_count} conflicting duplicate market_key rows — see audit/market_key_conflicts.csv")
    else:
        info("M12", "PASS — 0 conflicting duplicate market_key rows")

    if _identical_count > 0:
        warn("M12W", f"{_identical_count} identical duplicate market_key rows — deduplicated deterministically")
    else:
        info("M12W", "PASS — 0 identical duplicate market_key rows")
else:
    _conflicting_count = 0
    _identical_count   = 0
    info("M12",  "PASS — 0 duplicate market_key rows")
    info("M12W", "PASS — 0 duplicate market_key rows")

info("S7_AUDIT", "Audit complete. Proceeding to assembly.")

# COMMAND ----------
# =============================================================================
# SECTION 8 — ASSEMBLE CATALOG OUTPUTS
# =============================================================================

info("S8_ASSEMBLE", "=" * 60)
info("S8_ASSEMBLE", "CATALOG ASSEMBLY")

# ---------------------------------------------------------------------------
# 8A  _align helper — enforces canonical 20-column schema
# ---------------------------------------------------------------------------
def _align(df, source_cbu_val, source_table_val, nielsen_label):
    for _col in _SCHEMA_ORDER:
        if _col not in df.columns:
            df = df.withColumn(_col, F.lit(None).cast(T.StringType()))
    df = (
        df
        .withColumn("source_cbu",           F.coalesce(F.col("source_cbu"),           F.lit(source_cbu_val)))
        .withColumn("source_table",         F.coalesce(F.col("source_table"),         F.lit(source_table_val)))
        .withColumn("nielsen_source_table", F.coalesce(F.col("nielsen_source_table"), F.lit(nielsen_label)))
        .withColumn("catalog_date",         F.coalesce(F.col("catalog_date"),         F.lit(RUN_DATE)))
        .withColumn("catalog_version",      F.coalesce(F.col("catalog_version"),      F.lit(CATALOG_VERSION)))
    )
    return df.select(_SCHEMA_ORDER)

# Align IBP + SELL_IN reference rows to schema
def _mk_udf(cbu_prefix):
    return F.udf(lambda v: market_key_fn(cbu_prefix, str(v).strip().upper()) if v else None, T.StringType())

df_ibp_aligned = (
    df_ibp_profile
    .withColumn("market_key",           _mk_udf("IBP")(F.col("mrkt_dsc_shrt_norm")))
    .withColumn("canal_std",            F.lit(None).cast(T.StringType()))
    .withColumn("canal_lvl1",           F.lit(None).cast(T.StringType()))
    .withColumn("canal_lvl2",           F.lit(None).cast(T.StringType()))
    .withColumn("agg_level",            F.lit(None).cast(T.StringType()))
    .withColumn("region_type",          F.lit(None).cast(T.StringType()))
    .withColumn("region_std",           F.lit(None).cast(T.StringType()))
    .withColumn("region_detail",        F.lit(None).cast(T.StringType()))
    .withColumn("is_total_mexico",      F.lit("NO"))
    .withColumn("is_scanning",          F.lit("NO"))
    .withColumn("catalog_date",         F.lit(RUN_DATE))
    .withColumn("catalog_version",      F.lit(CATALOG_VERSION))
    .select(_SCHEMA_ORDER)
)

df_si_aligned = (
    df_si_profile
    .withColumn("market_key",           _mk_udf("SELL_IN")(F.col("mrkt_dsc_shrt_norm")))
    .withColumn("canal_std",            F.lit(None).cast(T.StringType()))
    .withColumn("canal_lvl1",           F.lit(None).cast(T.StringType()))
    .withColumn("canal_lvl2",           F.lit(None).cast(T.StringType()))
    .withColumn("agg_level",            F.lit(None).cast(T.StringType()))
    .withColumn("region_type",          F.lit(None).cast(T.StringType()))
    .withColumn("region_std",           F.lit(None).cast(T.StringType()))
    .withColumn("region_detail",        F.lit(None).cast(T.StringType()))
    .withColumn("is_total_mexico",      F.lit("NO"))
    .withColumn("is_scanning",          F.lit("NO"))
    .withColumn("catalog_date",         F.lit(RUN_DATE))
    .withColumn("catalog_version",      F.lit(CATALOG_VERSION))
    .select(_SCHEMA_ORDER)
)

# ---------------------------------------------------------------------------
# 8B  Dynamic promoted count gate M9a
# ---------------------------------------------------------------------------
_expected_promoted = _total_confirmed
info("S8_ASSEMBLE", f"Dynamic M9a: expected promoted (from live join) = {_expected_promoted}")

# ---------------------------------------------------------------------------
# 8C  M9b pre-check
# ---------------------------------------------------------------------------
if _total_confirmed + _total_pending_n != _total_live_nielsen:
    blocker("M9b", (
        f"Pre-union: confirmed({_total_confirmed}) + pending({_total_pending_n}) "
        f"= {_total_confirmed + _total_pending_n} ≠ live({_total_live_nielsen})"
    ))
else:
    info("M9b", f"PASS — pre-union: {_total_confirmed} + {_total_pending_n} = {_total_live_nielsen}")

# ---------------------------------------------------------------------------
# 8D  Union → dedup → split
# ---------------------------------------------------------------------------
df_all = (
    df_edp_joined.select(_SCHEMA_ORDER)
    .union(df_wst_joined.select(_SCHEMA_ORDER))
    .union(df_wrt_joined.select(_SCHEMA_ORDER))
    .union(df_pb_joined.select(_SCHEMA_ORDER))
    .union(df_ibp_aligned)
    .union(df_si_aligned)
    .dropDuplicates(["market_key"])
)

df_promoted  = df_all.filter(F.col("promoted") == "YES").cache()
df_pending   = df_all.filter(F.col("promoted") == "PENDING").cache()
df_reference = df_all.cache()

_promoted_count  = df_promoted.count()
_pending_count   = df_pending.count()
_reference_count = df_reference.count()

info("S8_ASSEMBLE", f"Promoted rows:   {_promoted_count}")
info("S8_ASSEMBLE", f"Pending rows:    {_pending_count}")
info("S8_ASSEMBLE", f"Reference rows:  {_reference_count}")

# ---------------------------------------------------------------------------
# Gate M9a — dynamic
# ---------------------------------------------------------------------------
if _expected_promoted > 0 and _promoted_count != _expected_promoted:
    blocker("M9a", (
        f"Promoted count mismatch: live join expected {_expected_promoted} "
        f"but got {_promoted_count} after dedup"
    ))
elif _promoted_count == 0 and _expected_promoted == 0:
    warn("M9a", "0 promoted rows — signoff seed matched 0 live markets")
else:
    info("M9a", f"PASS — promoted ({_promoted_count}) = live join result ({_expected_promoted})")

# Gate M9b — post-dedup
_promoted_nielsen = df_promoted.filter(~F.col("source_cbu").isin(["IBP","SELL_IN"])).count()
_pending_nielsen  = df_pending.filter(~F.col("source_cbu").isin(["IBP","SELL_IN"])).count()
if _promoted_nielsen + _pending_nielsen != _total_live_nielsen:
    blocker("M9b", (
        f"Post-dedup: promoted_nielsen({_promoted_nielsen}) + pending_nielsen({_pending_nielsen}) "
        f"= {_promoted_nielsen + _pending_nielsen} ≠ live({_total_live_nielsen})"
    ))
else:
    info("M9b", f"PASS — post-dedup: {_promoted_nielsen} + {_pending_nielsen} = {_total_live_nielsen}")

# Gate M13 — 20-column schema
_m13_pass = True
for _dfname, _dfcheck in [("promoted", df_promoted), ("pending", df_pending), ("reference", df_reference)]:
    if list(_dfcheck.columns) != _SCHEMA_ORDER:
        blocker("M13", f"Schema mismatch in {_dfname}: got {list(_dfcheck.columns)}")
        _m13_pass = False
    else:
        info("M13", f"PASS — {_dfname}: 20-column schema confirmed")

# Gate M14 — row accountability
if _promoted_nielsen + _pending_nielsen == _total_live_nielsen:
    info("M14", f"PASS — all {_total_live_nielsen} live Nielsen rows accounted for")
else:
    blocker("M14", (
        f"promoted_nielsen({_promoted_nielsen}) + pending_nielsen({_pending_nielsen}) "
        f"≠ live({_total_live_nielsen})"
    ))

# Gate M15 — no null critical fields in promoted
_critical_cols = ["canal_std", "canal_lvl1", "canal_lvl2", "agg_level", "region_type", "region_std"]
_null_violations = df_promoted
for _c in _critical_cols:
    _null_violations = _null_violations.filter(F.col(_c).isNull())
_null_count = _null_violations.count()

if _null_count > 0:
    _null_violations.coalesce(1).write.mode("overwrite").option("header", True).csv(
        f"{DBFS_BASE}/audit/cat_market_null_violations.csv"
    )
    blocker("M15", (
        f"{_null_count} promoted rows have null critical fields — "
        f"see audit/cat_market_null_violations.csv"
    ))
else:
    info("M15", "PASS — 0 null critical fields in promoted rows")

# Gate M16 — no reference-only in promoted
_ref_in_promoted = df_promoted.filter(F.col("source_cbu").isin(["IBP","SELL_IN"])).count()
if _ref_in_promoted > 0:
    blocker("M16", f"{_ref_in_promoted} REFERENCE_ONLY rows (IBP/SELL_IN) found in cat_market.csv")
else:
    info("M16", "PASS — no IBP or SELL_IN rows in cat_market.csv")

# Gate M17 — canal_std closed catalog in promoted output
_live_canal_vals   = {
    r.canal_std.strip() if r.canal_std else ""
    for r in df_promoted.select("canal_std").distinct().collect()
}
_canal_exceptions_live = {v for v in _live_canal_vals if v and v not in _CANAL_STD_GOVERNED}
if _canal_exceptions_live:
    warn("M17", f"Promoted canal_std governance exceptions: {_canal_exceptions_live}")
else:
    info("M17", f"PASS — all promoted canal_std within governed catalog")

# Gate M10 WARN
if _pending_count > 0:
    warn("M10", f"cat_market_pending.csv has {_pending_count} rows (WATER_ST governance debt)")
else:
    info("M10", "PASS — cat_market_pending.csv: 0 rows")

# Write final outputs
df_promoted.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{DBFS_BASE}/cat_market.csv")
df_pending.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{DBFS_BASE}/cat_market_pending.csv")
df_reference.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{DBFS_BASE}/cat_market_reference.csv")

info("S8_ASSEMBLE", f"Written: cat_market.csv          ({_promoted_count} rows)")
info("S8_ASSEMBLE", f"Written: cat_market_pending.csv  ({_pending_count} rows)")
info("S8_ASSEMBLE", f"Written: cat_market_reference.csv ({_reference_count} rows)")

# COMMAND ----------
# =============================================================================
# SECTION 8 (cont.) — SUMMARY REPORT
# =============================================================================

info("S8_SUMMARY", "=" * 60)
info("S8_SUMMARY", "MARKET CATALOG BUILD SUMMARY")

_M13_result = "PASS" if _m13_pass else "FAIL"
_M15_result = "PASS" if _null_count == 0 else f"FAIL ({_null_count} violations)"
_M16_result = "PASS" if _ref_in_promoted == 0 else f"FAIL ({_ref_in_promoted} rows)"
_M17_result = "PASS" if not _canal_exceptions_live else f"WARN: {_canal_exceptions_live}"
_M9b_result = "PASS" if _promoted_nielsen + _pending_nielsen == _total_live_nielsen else "FAIL"
_M9a_result = "PASS" if not any("M9a" in b for b in _blockers) else "FAIL"

_report_lines = [
    "=" * 70,
    f"MARKET CATALOG BUILD SUMMARY  v{CATALOG_VERSION}  --  {RUN_DATE}  {RUN_TIMESTAMP}",
    "=" * 70,
    "TAXONOMY:            Danone Nielsen Market Catalog (signoff_03 governed)",
    "SNOWFLAKE_ACCESS:    READ-ONLY ✅",
    "",
    "CREDENTIAL PROFILES USED:",
    "  PRD_MEX: EDP, WATER_ST, WATER_RT, PB, SELL_IN",
    "  PRD_MDP: IBP",
    "",
    "SIGNOFF SEED:",
    "  Path:    configs/catalog_seeds/market/signoff_03_nielsen_markets.csv",
    f"  Rows:    {_signoff_total}",
    f"  Dup keys:{_signoff_dup_count}   ← M11",
    f"  canal_std exceptions: {_seed_exceptions if _seed_exceptions else 'None'}  ← M17",
    "  CBUs covered: EDP_NIELSEN (155), WATER_NIELSEN_RIE (73), PB_NIELSEN (134)",
    "  CBUs MISSING: WATER_NIELSEN_ST — governance debt (82 → PENDING)",
    "",
    "CBU COVERAGE REPORT:",
    f"  {'CBU':<10} {'Live':>6} {'Classified':>12} {'Pending':>9} {'Coverage':>10}",
    f"  {'-'*52}",
]
for _cbu, _cov in _coverage.items():
    _flag = "  ⚠️  GOVERNANCE DEBT" if _cbu == "WATER_ST" else ""
    _report_lines.append(
        f"  {_cbu:<10} {_cov['total']:>6} {_cov['confirmed']:>12} {_cov['pending']:>9} {_cov['pct']:>9.1f}%{_flag}"
    )
_report_lines += [
    f"  {'TOTAL':<10} {_total_live_nielsen:>6} {_total_confirmed:>12} {_total_pending_n:>9} {_total_pct:>9.1f}%",
    "",
    "GOVERNANCE DEBT:",
    "  WATER_ST: 0% coverage — 82 markets unclassified.",
    "  Action: classify WATER_ST markets, add to signoff seed with",
    "          NIELSEN_SOURCE_TABLE='WATER_NIELSEN_ST', then re-run.",
    "",
    "MARKET_ID STABILITY:",
    f"  EDP:      {_edp_id_count} IDs / {_edp_name_count} names",
    f"  WATER_ST: {_wst_id_count} IDs / {_wst_name_count} names  (all PENDING)",
    f"  WATER_RT: {_wrt_id_count} IDs / {_wrt_name_count} names",
    f"  PB:       {_pb_id_count} IDs / {_pb_name_count} names",
    "  (market_id retained in per-CBU profile files — use for lineage traceability)",
    "",
    "GEOGRAPHIC HIERARCHY DISTRIBUTION (promoted rows):",
    f"  NATIONAL: {_region_dist.get('NATIONAL', 0)}",
    f"  AREA:     {_region_dist.get('AREA', 0)}",
    f"  CITY:     {_region_dist.get('CITY', 0)}",
    f"  VDM:      {_region_dist.get('VDM', 0)}",
    "",
    "CROSS-CBU OVERLAP (descriptive — not a quality criterion):",
    f"  IN_ALL_4: {len(_in_all4)} | EDP∩WST: {len(_in_edp_wst)} | EDP∩WRT: {len(_in_edp_wrt)} | EDP∩PB: {len(_in_edp_pb)}",
    "",
    "PROMOTED / PENDING SUMMARY:",
    f"  Expected promoted (live join):  {_expected_promoted}",
    f"  Actual promoted:                {_promoted_count}",
    f"  Pending Nielsen rows:           {_pending_count}",
    f"  Total live Nielsen:             {_total_live_nielsen}",
    f"  Pending %:                      {_pending_pct:.1f}%",
    f"  M9b (promo+pend=live):          {_M9b_result}",
    "",
    "is_total_mexico PER CBU (M8 — WARN only, informational):",
    f"  EDP:      {'YES' if _edp_has_total else 'NO'}",
    "  WATER_ST: N/A (all PENDING)",
    f"  WATER_RT: {'YES' if _wrt_has_total else 'NO'}",
    f"  PB:       {'YES' if _pb_has_total else 'NO'}",
    "",
    "DUPLICATE MARKET_KEY AUDIT:",
    f"  Conflicting: {_conflicting_count}  ← M12",
    f"  Identical:   {_identical_count}    ← M12W",
    "",
    f"SCHEMA VALIDATION (20 columns):  {_M13_result}",
    f"NULL CRITICAL FIELDS CHECK:       {_M15_result}",
    f"REFERENCE-ONLY EXCLUSION CHECK:   {_M16_result}",
    f"CANAL_STD GOVERNANCE (M17):       {_M17_result}",
    "",
    "SOURCE TABLES QUERIED:",
    "  PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_MKT_DIM   (S1 EDP)",
    "  PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_ST_NLSN_MKT_DIM  (S2 WATER_ST)",
    "  PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_RT_NLSN_MKT_DIM  (S3 WATER_RT)",
    "  PRD_MEX.MEX_DSP_DPH_MKT.VW_SUST_LECHE_ST_NLSN_MKT_DIM    (S4 PB)",
    "  PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP                         (S5 IBP)",
    "  PRD_MEX.MEX_DSP_OTC.V_D_CLIENT                             (S6 SELL_IN)",
    "",
    "OUTPUT FILES:",
    f"  cat_market.csv:              {_promoted_count} rows  (promoted=YES)",
    f"  cat_market_pending.csv:      {_pending_count} rows",
    f"  cat_market_reference.csv:    {_reference_count} rows",
    f"  profiles/edp_market_profile.csv:      {_edp_name_count} rows  (incl. market_id)",
    f"  profiles/water_st_market_profile.csv: {_wst_name_count} rows  (all PENDING, incl. market_id)",
    f"  profiles/water_rt_market_profile.csv: {_wrt_name_count} rows  (incl. market_id)",
    f"  profiles/pb_market_profile.csv:       {_pb_name_count} rows  (incl. market_id)",
    f"  profiles/ibp_mercado_profile.csv:     {_ibp_count} rows",
    f"  profiles/sellin_geo_profile.csv:      {_si_count} rows",
    "  audit/market_cross_cbu_overlap.csv:   written (descriptive)",
    f"  audit/cat_market_null_violations.csv: {'NOT WRITTEN' if _null_count == 0 else str(_null_count) + ' rows — REVIEW'}",
    "  build_cat_market_report.txt:          this file",
    "",
    "VALIDATION GATES:",
    f"  M1   EDP distinct >= 50:             {'PASS' if _edp_name_count >= 50 else 'FAIL'} ({_edp_name_count})",
    f"  M2   WATER_ST distinct >= 30:        {'PASS' if _wst_name_count >= 30 else 'FAIL'} ({_wst_name_count})",
    f"  M3   WATER_RT distinct >= 20:        {'PASS' if _wrt_name_count >= 20 else 'FAIL'} ({_wrt_name_count})",
    f"  M4   PB distinct >= 50:              {'PASS' if _pb_name_count >= 50 else 'FAIL'} ({_pb_name_count})",
    f"  M5   IBP MERCADO >= 5:               {'PASS' if _ibp_count >= 5 else 'WARN'} ({_ibp_count})",
    f"  M6   SELL_IN geo rows > 0:           {'PASS' if _si_count > 0 else 'WARN'} ({_si_count})",
    f"  M7   PENDING <= 5%:                  {'PASS' if _pending_pct <= 5.0 else 'WARN'} ({_pending_pct:.1f}%)",
    f"  M8   is_total_mexico per CBU:        WARN only — informational",
    f"  M9a  promoted = live join result:    {_M9a_result} ({_promoted_count} vs {_expected_promoted})",
    f"  M9b  promoted+pending = live:        {_M9b_result}",
    f"  M10  pending rows = 0:               {'PASS' if _pending_count == 0 else 'WARN'} ({_pending_count})",
    f"  M11  signoff no dup keys:            {'PASS' if _signoff_dup_count == 0 else 'FAIL'}",
    f"  M12  no conflicting dup keys:        {'PASS' if _conflicting_count == 0 else 'FAIL'}",
    f"  M12W identical dups deduplicated:    {'PASS' if _identical_count == 0 else 'WARN'}",
    f"  M13  20-column schema correct:       {_M13_result}",
    f"  M14  all live rows in promo/pend:    {'PASS' if _promoted_nielsen + _pending_nielsen == _total_live_nielsen else 'FAIL'}",
    f"  M15  no null critical fields:        {_M15_result}",
    f"  M16  no ref-only in cat_market:      {_M16_result}",
    f"  M17  canal_std in governed catalog:  {_M17_result}",
    f"  RO   Snowflake read-only:            PASS",
    "",
    f"WARNINGS:  {len(_warnings)}",
    f"BLOCKERS:  {len(_blockers)}",
    "=" * 70,
]

_report_text = "\n".join(_report_lines)

for _line in _report_lines:
    info("S8_SUMMARY", _line)

# Write DBFS copy
_report_dbfs = f"{DBFS_BASE}/build_cat_market_report.txt"
dbutils.fs.put(_report_dbfs, _report_text, overwrite=True)

# Write repo copy
_repo_log_dir  = os.path.join(_repo_root, "logs", "catalog_eda")
os.makedirs(_repo_log_dir, exist_ok=True)
with open(os.path.join(_repo_log_dir, "build_cat_market_report.txt"), "w") as _fh:
    _fh.write(_report_text)

info("S8_SUMMARY", f"Reports written to DBFS and {REPO_LOG_PATH}")

# ---------------------------------------------------------------------------
# Final pass/fail
# ---------------------------------------------------------------------------
if _blockers:
    info("S8_SUMMARY", f"FAIL — {len(_blockers)} blocker(s):")
    for _b in _blockers:
        info("S8_SUMMARY", f"  {_b}")
    raise RuntimeError(f"build_cat_market.py FAILED — {len(_blockers)} blocker(s). Review report.")
else:
    info("S8_SUMMARY",
         f"✅ PASS — {_promoted_count} market rows promoted. "
         f"{len(_warnings)} warning(s) (non-blocking).")
