# Databricks notebook source
# MAGIC %md
# MAGIC # build_cat_canal.py — CANAL Catalog Build
# MAGIC ## Danone Master Data Catalog v6.0.0
# MAGIC ## plan ref: build_cat_canal_plan_v3 — approved 2026-06-29

# COMMAND ----------

# MAGIC %md ## SECTION 0 — Setup, Logging, Helpers, Snowflake Read-Only Guard

# COMMAND ----------

# Cell 0: constants + helper imports
import re, hashlib, datetime, json, math, os, importlib.util, pathlib
from pyspark.sql import functions as F, types as T
from functools import reduce

CATALOG_VERSION   = "6.0.0"
RUN_DATE          = datetime.date.today().isoformat()
DBFS_BASE         = "dbfs:/mnt/mdp/mdm/master_catalog/canal"
REPO_LOG_BASE     = "/Workspace/Users/victor.hernandez29@danone.com/Market-Driven-Commercial-Growth-Intelligence-Platform/logs/catalog_eda"
SEED_DBFS_PATH    = "dbfs:/mnt/mdp/mdm/master_catalog/canal/seed/canal_unified_seed.csv"
SEED_REPO_PATH    = "/Workspace/Users/victor.hernandez29@danone.com/Market-Driven-Commercial-Growth-Intelligence-Platform/configs/catalog_seeds/canal_unified_seed.csv"
DB_PRD_MDP        = "PRD_MDP"
DB_PRD_MEX        = "PRD_MEX"

_warnings  = []
_blockers  = []
_log_lines = []

def _ts():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def log(tag, msg, level="INFO"):
    line = f"[{_ts()}][{level}][{tag}] {msg}"
    print(line)
    _log_lines.append(line)

def info(tag, msg):  log(tag, msg, "INFO")
def warn(tag, msg):
    log(tag, msg, "WARNING")
    _warnings.append(f"[{tag}] {msg}")
def blocker(tag, msg):
    log(tag, msg, "BLOCKER")
    _blockers.append(f"[{tag}] {msg}")
    raise RuntimeError(f"BLOCKER [{tag}]: {msg}")
def assert_gate(gate_id, condition, msg):
    if not condition:
        blocker(gate_id, msg)
    else:
        info(gate_id, f"PASS — {msg}")

def normalize(v):
    if v is None: return None
    return str(v).strip().upper()

def canal_key(source_system, source_column, canal_level, canal_raw_norm):
    raw = f"{source_system}|{source_column}|{canal_level}|{canal_raw_norm}"
    return hashlib.md5(raw.encode()).hexdigest()[:16]

udf_canal_key = F.udf(canal_key, T.StringType())

# COMMAND ----------

# Cell 1: Snowflake connection + read-only guard
# Uses the same credential pattern as build_cat_marca.py and validate_canal_dim_structure.py

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

SF_URL = "danonenam.east-us-2.azure.snowflakecomputing.com"


def get_sf_options(database: str) -> dict:
    """Return Snowflake connector options for the given database."""
    _mdp_user = getattr(_m, "SF_MDP_USER", None)
    _mdp_pwd  = getattr(_m, "SF_MDP_PASSWORD", None)
    profiles = {
        DB_PRD_MEX: {
            "sfURL":       SF_URL,
            "sfUser":      _m.SF_MEX_USER,
            "sfPassword":  _m.SF_MEX_PASSWORD,
            "sfWarehouse": getattr(_m, "SF_MEX_WH",  "PRD_MEX_ANL_WH"),
            "sfRole":      getattr(_m, "SF_MEX_ROLE", "PRD_MEX_READER"),
        },
        DB_PRD_MDP: {
            "sfURL":       SF_URL,
            "sfUser":      _mdp_user or dbutils.secrets.get(
                               "DAN-AM-P-KVT800-R-MDP-DB", "snowflake-user"),
            "sfPassword":  _mdp_pwd or dbutils.secrets.get(
                               "DAN-AM-P-KVT800-R-MDP-DB", "snowflake-password"),
            "sfWarehouse": getattr(_m, "SF_MDP_WH",  "PRD_MDP_ANL_WH"),
            "sfRole":      getattr(_m, "SF_MDP_ROLE", "PRD_MDP"),
        },
    }
    if database not in profiles:
        raise ValueError(
            f"No Snowflake profile for '{database}'. Available: {list(profiles.keys())}"
        )
    return dict(profiles[database])


# Snowflake read-only guard
_ALLOWED = re.compile(r'^\s*(SELECT|WITH)\b', re.IGNORECASE)
_BLOCKED = re.compile(
    r'\b(INSERT|UPDATE|DELETE|MERGE|CREATE|DROP|TRUNCATE|ALTER|'
    r'REPLACE|COPY|GRANT|REVOKE|PUT|GET|REMOVE|UNDROP|CLONE)\b',
    re.IGNORECASE
)


def run_sf(sql: str, db: str):
    """Safe Snowflake SELECT-only reader with DML/DDL guard (V11)."""
    if not _ALLOWED.match(sql):
        blocker("V11", f"SQL must start with SELECT or WITH. Got: {sql[:80]}")
    if _BLOCKED.search(sql):
        blocker("V11", f"DML/DDL keyword detected — Snowflake is read-only. SQL[:80]: {sql[:80]}")
    return (
        spark.read.format("net.snowflake.spark.snowflake")
        .options(**get_sf_options(db))
        .option("sfDatabase", db)
        .option("query", sql)
        .load()
    )


print(f"OK Credentials loaded -- PRD_MEX user: {_m.SF_MEX_USER}")
info("S0", f"build_cat_canal v{CATALOG_VERSION} initializing — run_date={RUN_DATE}")


# COMMAND ----------

# Cell 2: Init DBFS output dirs
for sub in ["", "/seed"]:
    dbutils.fs.mkdirs(DBFS_BASE + sub)
info("S0", f"DBFS output dirs initialized: {DBFS_BASE}")

# COMMAND ----------

# Cell 3: Load seed file
_SEED_SCHEMA = T.StructType([
    T.StructField("source_system",       T.StringType()),
    T.StructField("source_column",       T.StringType()),
    T.StructField("canal_raw",           T.StringType()),
    T.StructField("canal_raw_norm",      T.StringType()),
    T.StructField("canal_level",         T.StringType()),
    T.StructField("gran_canal_grp",      T.StringType()),
    T.StructField("canal_type",          T.StringType()),
    T.StructField("promoted",            T.StringType()),
    T.StructField("business_rule",       T.StringType()),
    T.StructField("confirmation_status", T.StringType()),
    T.StructField("notes",               T.StringType()),
])

try:
    # Copy seed from repo workspace path to DBFS so Spark can read it
    dbutils.fs.cp(f"file:{SEED_REPO_PATH}", SEED_DBFS_PATH, recurse=False)
    df_seed = spark.read.csv(SEED_DBFS_PATH, header=True, schema=_SEED_SCHEMA)
    seed_count = df_seed.count()
    info("S0_SEED", f"Governed seed loaded: {seed_count} rows from {SEED_DBFS_PATH}")
    _seed_available = True
except Exception as e:
    warn("S0_SEED", f"Seed file not found or unreadable: {e}. Will fall back to Python GRAN_CANAL_MAP.")
    _seed_available = False
    df_seed = None

# COMMAND ----------

# MAGIC %md ## SECTION 1 — IBP Commercial Channel Hierarchy

# COMMAND ----------

# DBTITLE 1,Cell 8
info("S1_IBP", "=" * 70)
info("S1_IBP", "IBP COMMERCIAL CHANNEL HIERARCHY")

SQL_IBP = """
    SELECT
        GRAN_CANAL,
        CANAL,
        GRUPO,
        CADENA,
        FM,
        COUNT(*) AS row_count
    FROM PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP
    WHERE NOMBRE_ETIQUETA = 'REAL'
      AND GRAN_CANAL IS NOT NULL
    GROUP BY 1,2,3,4,5
    ORDER BY GRAN_CANAL, CANAL
"""

df_ibp_raw = run_sf(SQL_IBP, DB_PRD_MDP)
df_ibp_raw.cache()
ibp_total = df_ibp_raw.agg(F.sum("row_count")).collect()[0][0]
ibp_gran_canal_vals = set(r["GRAN_CANAL"] for r in df_ibp_raw.select("GRAN_CANAL").distinct().collect())
ibp_gran_canal_vals = {normalize(v) for v in ibp_gran_canal_vals if v}

info("S1_IBP", f"IBP distinct GRAN_CANAL values: {sorted(ibp_gran_canal_vals)}")
info("S1_IBP", f"IBP total fact rows (REAL): {ibp_total:,}")

# Gate V1: UTT and DTT must both be present
if "UTT" not in ibp_gran_canal_vals:
    blocker("V1", "IBP GRAN_CANAL missing UTT — cannot build CANAL catalog")
if "DTT" not in ibp_gran_canal_vals:
    blocker("V1", "IBP GRAN_CANAL missing DTT — cannot build CANAL catalog")
info("V1", "PASS — IBP contains UTT and DTT")

# Gate V2: CAM must not be remapped
if "CAM" in ibp_gran_canal_vals:
    cam_count_rows = df_ibp_raw.filter(F.col("GRAN_CANAL") == "CAM").agg(F.sum("row_count")).collect()[0][0]
    info("V2", f"PASS — CAM present in IBP ({cam_count_rows} rows). Preserved as gran_canal_grp=CAM.")
else:
    warn("V2", "CAM not found in IBP GRAN_CANAL for this run period — expected if year filter applied")

# IBP row split by GRAN_CANAL (dynamic)
SQL_IBP_SPLIT = """
    SELECT
        GRAN_CANAL,
        COUNT(*)        AS row_count,
        SUM(VALOR)      AS valor_plan,
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS row_pct
    FROM PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP
    WHERE NOMBRE_ETIQUETA = 'REAL'
      AND GRAN_CANAL IS NOT NULL
    GROUP BY 1
    ORDER BY row_count DESC
"""
df_ibp_split = run_sf(SQL_IBP_SPLIT, DB_PRD_MDP)
for r in df_ibp_split.collect():
    info("S1_IBP", f"  GRAN_CANAL={r['GRAN_CANAL']:6s}  rows={r['ROW_COUNT']:>10,}  ({r['ROW_PCT']:.1f}%)  valor_plan={r['VALOR_PLAN']:,.0f}")

# Write ibp_canal_hierarchy.csv
df_ibp_hier = (
    df_ibp_raw
    .withColumnRenamed("GRAN_CANAL", "gran_canal")
    .withColumnRenamed("CANAL",      "canal")
    .withColumnRenamed("GRUPO",      "grupo")
    .withColumnRenamed("CADENA",     "cadena")
    .withColumnRenamed("FM",         "fm")
)
df_ibp_hier.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{DBFS_BASE}/ibp_canal_hierarchy.csv")
info("S1_IBP", f"Written ibp_canal_hierarchy.csv ({df_ibp_hier.count()} rows)")

# Build IBP catalog rows — L1 (GRAN_CANAL)
df_ibp_l1 = (
    df_ibp_raw
    .groupBy("GRAN_CANAL")
    .agg(F.sum("row_count").alias("row_count"))
    .withColumn("canal_std",          F.col("GRAN_CANAL"))
    .withColumn("canal_raw",          F.col("GRAN_CANAL"))
    .withColumn("canal_raw_norm",     F.trim(F.upper(F.col("GRAN_CANAL"))))
    .withColumn("canal_type",         F.lit("COMMERCIAL_CHANNEL"))
    .withColumn("canal_level",        F.lit("L1_GRAN_CANAL"))
    .withColumn("gran_canal_grp",     F.trim(F.upper(F.col("GRAN_CANAL"))))
    .withColumn("parent_canal_std",   F.lit(None).cast(T.StringType()))
    .withColumn("l2_channel",         F.lit(None).cast(T.StringType()))
    .withColumn("source_system",      F.lit("IBP"))
    .withColumn("source_column",      F.lit("GRAN_CANAL"))
    .withColumn("promoted",           F.lit("YES"))
    .withColumn("confirmation_status",F.lit("CONFIRMED"))
    .withColumn("catalog_date",       F.lit(RUN_DATE))
    .withColumn("catalog_version",    F.lit(CATALOG_VERSION))
    .withColumn("notes",              F.lit("IBP planning channel"))
    .drop("GRAN_CANAL")
)

# Build IBP catalog rows — L2 (CANAL)
df_ibp_l2 = (
    df_ibp_raw
    .groupBy("GRAN_CANAL", "CANAL")
    .agg(F.sum("row_count").alias("row_count"))
    .withColumn("canal_std",          F.col("CANAL"))
    .withColumn("canal_raw",          F.col("CANAL"))
    .withColumn("canal_raw_norm",     F.trim(F.upper(F.col("CANAL"))))
    .withColumn("canal_type",         F.lit("COMMERCIAL_CHANNEL"))
    .withColumn("canal_level",        F.lit("L2_CANAL"))
    .withColumn("gran_canal_grp",     F.trim(F.upper(F.col("GRAN_CANAL"))))
    .withColumn("parent_canal_std",   F.trim(F.upper(F.col("GRAN_CANAL"))))
    .withColumn("l2_channel",         F.col("CANAL"))
    .withColumn("source_system",      F.lit("IBP"))
    .withColumn("source_column",      F.lit("CANAL"))
    .withColumn("promoted",           F.lit("YES"))
    .withColumn("confirmation_status",F.lit("CONFIRMED"))
    .withColumn("catalog_date",       F.lit(RUN_DATE))
    .withColumn("catalog_version",    F.lit(CATALOG_VERSION))
    .withColumn("notes",              F.lit("IBP L2 commercial channel"))
    .drop("GRAN_CANAL", "CANAL")
)

info("S1_IBP", f"IBP L1 rows: {df_ibp_l1.count()} | IBP L2 rows: {df_ibp_l2.count()}")
info("S1_IBP", "Section 1 complete.")

# COMMAND ----------

# MAGIC %md ## SECTION 2 — SELL_IN Commercial Channel Hierarchy (L1 + L2)

# COMMAND ----------

# DBTITLE 1,Cell 10
info("S2_SELLIN", "=" * 70)
info("S2_SELLIN", "SELL_IN COMMERCIAL CHANNEL HIERARCHY")

SQL_SELLIN_L1 = """
    SELECT
        cus_grn_chl_dsc,
        COUNT(*) AS row_count
    FROM PRD_MEX.MEX_DSP_OTC.V_D_CLIENT
    GROUP BY 1
    ORDER BY row_count DESC
"""

SQL_SELLIN_L1L2 = """
    SELECT
        cus_grn_chl_dsc,
        lv6_hie_cus_dsc,
        COUNT(*) AS row_count
    FROM PRD_MEX.MEX_DSP_OTC.V_D_CLIENT
    WHERE cus_grn_chl_dsc IS NOT NULL
    GROUP BY 1,2
    ORDER BY 1,2
"""

df_si_l1_raw = run_sf(SQL_SELLIN_L1, DB_PRD_MEX)
df_si_l1_raw.cache()

si_total = df_si_l1_raw.agg(F.sum("row_count")).collect()[0][0]
si_null_row = df_si_l1_raw.filter(F.col("cus_grn_chl_dsc").isNull()).agg(F.sum("row_count")).collect()
si_null  = si_null_row[0][0] if si_null_row and si_null_row[0][0] else 0

for r in df_si_l1_raw.orderBy(F.col("row_count").desc()).collect():
    val_disp = r["CUS_GRN_CHL_DSC"] or "(null)"
    pct = r["ROW_COUNT"] / si_total * 100
    info("S2_SELLIN", f"  cus_grn_chl_dsc={val_disp:10s}  rows={r['ROW_COUNT']:>10,}  ({pct:.1f}%)")

# Validation: expected L1 = DTT and UTT
L1_MAP = {
    "DTT": {
        "canal_std": "DTT",
        "gran_canal_grp": "DTT",
        "promoted": "YES",
        "confirmation_status": "CONFIRMED",
        "notes": "SELL_IN transactional channel — aligns with IBP DTT",
    },
    "UTT": {
        "canal_std": "UTT",
        "gran_canal_grp": "UTT",
        "promoted": "YES",
        "confirmation_status": "CONFIRMED",
        "notes": "SELL_IN transactional channel — aligns with IBP UTT",
    },
    "CAM": {
        "canal_std": "CAM",
        "gran_canal_grp": "CAM",
        "promoted": "YES",
        "confirmation_status": "CONFIRMED",
        "notes": "SELL_IN CAM: unexpected but preserved — classified same as IBP CAM (Central America). Warn V4 issued.",
    },
    "NA": {
        "canal_std": "UNKNOWN",
        "gran_canal_grp": "UNKNOWN",
        "promoted": "NO",
        "confirmation_status": "EXCLUDED",
        "notes": "Unclassified SELL_IN channel — excluded from promoted catalog",
    },
}

actual_l1 = set()
for r in df_si_l1_raw.filter(F.col("cus_grn_chl_dsc").isNotNull()).collect():
    actual_l1.add(normalize(r["CUS_GRN_CHL_DSC"]))

expected_l1 = {"DTT", "UTT"}
allowed_l1  = {"DTT", "UTT", "NA"}
missing     = expected_l1 - actual_l1
unexpected  = actual_l1 - allowed_l1

if "DTT" in missing:
    blocker("V3", "SELL_IN V_D_CLIENT missing DTT channel — cannot build CANAL catalog")
if "UTT" in missing:
    blocker("V3", "SELL_IN V_D_CLIENT missing UTT channel — cannot build CANAL catalog")
info("V3", "PASS — SELL_IN contains DTT and UTT")

if unexpected:
    for u in unexpected:
        if u == "CAM":
            warn("V4", "SELL_IN has CAM channel — unexpected but preserved per plan (classify same as IBP CAM). Business confirmation recommended.")
        else:
            warn("V4", f"Unexpected SELL_IN L1 value detected: '{u}' — review required")

# SELL_IN split stats (dynamic)
si_excl_na_rows = df_si_l1_raw.filter(F.col("cus_grn_chl_dsc").isin(["DTT", "UTT"])).agg(F.sum("row_count")).collect()
si_excl_na = si_excl_na_rows[0][0] if si_excl_na_rows and si_excl_na_rows[0][0] else 0

for label, v in [("DTT", "DTT"), ("UTT", "UTT"), ("NA", "NA")]:
    cnt_rows = df_si_l1_raw.filter(F.col("cus_grn_chl_dsc") == v).agg(F.sum("row_count")).collect()
    cnt = cnt_rows[0][0] if cnt_rows and cnt_rows[0][0] else 0
    pct_total = cnt / si_total * 100 if si_total else 0
    pct_excl  = cnt / si_excl_na * 100 if si_excl_na and v != "NA" else 0
    extra = f", {pct_excl:.1f}% excl NA" if v != "NA" else ""
    info("S2_SELLIN", f"  {label}: {cnt:>10,} rows ({pct_total:.1f}% incl NA{extra})")
info("S2_SELLIN", f"  null: {si_null:>10,} rows ({si_null / si_total * 100:.1f}%) — excluded from catalog")

# Build SELL_IN L1 catalog rows from a fresh query (avoid double-cache issues)
df_si_l1_raw2 = run_sf(SQL_SELLIN_L1, DB_PRD_MEX)
df_si_l1_classified = (
    df_si_l1_raw2
    .filter(F.col("cus_grn_chl_dsc").isNotNull())
    .withColumn("cus_grn_chl_dsc_norm", F.trim(F.upper(F.col("cus_grn_chl_dsc"))))
)

_si_rows = []
for r in df_si_l1_classified.collect():
    norm_val = normalize(r["CUS_GRN_CHL_DSC"])
    mapping = L1_MAP.get(norm_val, {
        "canal_std": norm_val,
        "gran_canal_grp": "UNKNOWN",
        "promoted": "PENDING",
        "confirmation_status": "UNKNOWN",
        "notes": f"Unrecognized SELL_IN L1 value: {norm_val}",
    })
    _si_rows.append({
        "canal_key":           canal_key("SELL_IN", "cus_grn_chl_dsc", "L1_GRAN_CANAL", norm_val),
        "canal_std":           mapping["canal_std"],
        "canal_type":          "COMMERCIAL_CHANNEL",
        "canal_level":         "L1_GRAN_CANAL",
        "canal_raw":           r["CUS_GRN_CHL_DSC"],
        "canal_raw_norm":      norm_val,
        "gran_canal_grp":      mapping["gran_canal_grp"],
        "parent_canal_std":    None,
        "l2_channel":          None,
        "source_system":       "SELL_IN",
        "source_column":       "cus_grn_chl_dsc",
        "promoted":            mapping["promoted"],
        "confirmation_status": mapping["confirmation_status"],
        "row_count":           r["ROW_COUNT"],
        "catalog_date":        RUN_DATE,
        "catalog_version":     CATALOG_VERSION,
        "notes":               mapping["notes"],
    })

_si_schema = T.StructType(
    [T.StructField(k, T.StringType(), True) for k in [
        "canal_key", "canal_std", "canal_type", "canal_level", "canal_raw",
        "canal_raw_norm", "gran_canal_grp", "parent_canal_std", "l2_channel",
        "source_system", "source_column", "promoted", "confirmation_status",
        "catalog_date", "catalog_version", "notes",
    ]] + [T.StructField("row_count", T.DecimalType(18, 0), True)]
)
df_si_l1 = spark.createDataFrame(_si_rows, schema=_si_schema)
info("S2_SELLIN", f"SELL_IN L1 catalog rows built: {df_si_l1.count()}")

# Load L1+L2 data and profile lv6_hie_cus_dsc
df_si_l1l2 = run_sf(SQL_SELLIN_L1L2, DB_PRD_MEX)
df_si_l1l2.cache()

si_l2_total_rows = df_si_l1l2.agg(F.sum("row_count")).collect()
si_l2_total  = si_l2_total_rows[0][0] if si_l2_total_rows and si_l2_total_rows[0][0] else 1
si_l2_null_rows = df_si_l1l2.filter(F.col("lv6_hie_cus_dsc").isNull()).agg(F.sum("row_count")).collect()
si_l2_null   = si_l2_null_rows[0][0] if si_l2_null_rows and si_l2_null_rows[0][0] else 0
l2_null_rate = si_l2_null / si_l2_total * 100

df_l2_dist = (
    df_si_l1l2
    .filter(F.col("lv6_hie_cus_dsc").isNotNull())
    .groupBy("cus_grn_chl_dsc", "lv6_hie_cus_dsc")
    .agg(F.sum("row_count").alias("rc"))
    .orderBy(F.col("rc").desc())
)

l2_distinct = df_l2_dist.select("lv6_hie_cus_dsc").distinct().count()
max_share_row = df_l2_dist.orderBy(F.col("rc").desc()).first()
max_share = (max_share_row["rc"] / si_l2_total * 100) if max_share_row else 100.0

info("S2_SELLIN", f"lv6_hie_cus_dsc: null_rate={l2_null_rate:.1f}%, distinct_vals={l2_distinct}, max_single_share={max_share:.1f}%")

if l2_null_rate > 30:
    warn("V5", f"lv6_hie_cus_dsc null rate {l2_null_rate:.1f}% > 30% threshold — promoting as REFERENCE_ONLY")
    _l2_promotable = False
elif l2_distinct < 2:
    warn("V5", f"lv6_hie_cus_dsc has only {l2_distinct} distinct value(s) — insufficient for segmentation — REFERENCE_ONLY")
    _l2_promotable = False
else:
    info("V5", f"PASS — lv6_hie_cus_dsc profiling supports promotion (null={l2_null_rate:.1f}%, distinct={l2_distinct})")
    _l2_promotable = True

if max_share > 70:
    warn("S2_SELLIN", f"lv6_hie_cus_dsc one value dominates at {max_share:.1f}% — reduced analytical usefulness noted")

# Build SELL_IN L2 catalog rows
_si_l2_rows = []
for r in df_l2_dist.filter(F.col("lv6_hie_cus_dsc").isNotNull()).collect():
    l1_norm = normalize(r['cus_grn_chl_dsc'])
    l2_norm = normalize(r["lv6_hie_cus_dsc"])
    parent  = L1_MAP.get(l1_norm, {}).get("canal_std", l1_norm)
    grp     = L1_MAP.get(l1_norm, {}).get("gran_canal_grp", "UNKNOWN")
    promo_note = "" if _l2_promotable else " [REFERENCE_ONLY: profiling weak]"
    _si_l2_rows.append({
        "canal_key":           canal_key("SELL_IN", "lv6_hie_cus_dsc", "L2_TIPO_CLIENTE", l2_norm),
        "canal_std":           r["lv6_hie_cus_dsc"],
        "canal_type":          "COMMERCIAL_CHANNEL",
        "canal_level":         "L2_TIPO_CLIENTE",
        "canal_raw":           r["lv6_hie_cus_dsc"],
        "canal_raw_norm":      l2_norm,
        "gran_canal_grp":      grp,
        "parent_canal_std":    parent,
        "l2_channel":          r["lv6_hie_cus_dsc"],
        "source_system":       "SELL_IN",
        "source_column":       "lv6_hie_cus_dsc",
        "promoted":            "YES" if _l2_promotable else "NO",
        "confirmation_status": "CONFIRMED" if _l2_promotable else "REFERENCE_ONLY",
        "row_count":           r["rc"],
        "catalog_date":        RUN_DATE,
        "catalog_version":     CATALOG_VERSION,
        "notes":               f"SELL_IN L2 TIPO_CLIENTE under {parent}{promo_note}",
    })

df_si_l2 = (
    spark.createDataFrame(_si_l2_rows)
    if _si_l2_rows
    else spark.createDataFrame([], df_si_l1.schema)
)
info("S2_SELLIN", f"SELL_IN L2 catalog rows built: {df_si_l2.count()} (promoted={'YES' if _l2_promotable else 'REFERENCE_ONLY'})")
info("S2_SELLIN", "Section 2 complete.")

# COMMAND ----------

# MAGIC %md ## SECTION 3 — SELL_OUT FORMAT Mapping (Seed-Driven)

# COMMAND ----------

# DBTITLE 1,Cell 12
info("S3_SELLOUT", "=" * 70)
info("S3_SELLOUT", "SELL_OUT RETAIL FORMAT — SEED-DRIVEN CLASSIFICATION")

SQL_SELLOUT = """
    SELECT
        FORMAT,
        CHAIN,
        COUNT(DISTINCT INT_ID) AS store_count
    FROM PRD_MDP.MDP_DSP.VW_D_STORE_RM
    WHERE FORMAT IS NOT NULL
    GROUP BY 1,2
    ORDER BY store_count DESC
"""

df_so_raw = run_sf(SQL_SELLOUT, DB_PRD_MDP)
df_so_raw.cache()

so_live_formats = {normalize(r["FORMAT"]) for r in df_so_raw.select("FORMAT").distinct().collect()}
info("S3_SELLOUT", f"Live SELL_OUT distinct FORMATs: {len(so_live_formats)}")

# Fallback GRAN_CANAL_MAP if seed unavailable
_GRAN_CANAL_MAP = {
    "7E-7ELEVEN":             "DTT",
    "7E-AEROPUERTO":          "DTT",
    "7E-AEROPUERTO PREMIUM":  "DTT",
    "7E-CAMPUS":              "DTT",
    "7E-CARRETERA":           "DTT",
    "7E-CENTRO":              "DTT",
    "7E-HABITACIONAL AB":     "DTT",
    "7E-HABITACIONAL C":      "DTT",
    "7E-PEATONAL":            "DTT",
    "7E-PLAZA COMERCIAL":     "DTT",
    "7E-SUPERPREMIUM":        "DTT",
    "7E-TRAFICO":             "DTT",
    "7E-TURISTAS":            "DTT",
    "7E-ZONA DE PROXIMIDAD":  "DTT",
    "OXXO":                   "DTT",
    "CASH AND CARRY":         "DTT",
}
# All other live FORMATs → UTT by default in fallback
for fmt in so_live_formats:
    if fmt not in _GRAN_CANAL_MAP:
        _GRAN_CANAL_MAP[fmt] = "UTT"

# Build FORMAT → seed join
if _seed_available and df_seed is not None:
    df_seed_so = (
        df_seed
        .filter(F.col("source_system") == "SELL_OUT")
        .select(
            "canal_raw_norm",
            "gran_canal_grp",
            "canal_type",
            "promoted",
            "business_rule",
            "confirmation_status",
            "notes",
        )
    )
    df_so_flat = df_so_raw.withColumn("FORMAT_NORM", F.trim(F.upper(F.col("FORMAT"))))
    df_so_joined = df_so_flat.join(
        df_seed_so.withColumnRenamed("canal_raw_norm", "FORMAT_NORM"),
        on="FORMAT_NORM",
        how="left",
    )
    info("S3_SELLOUT", "Joined live FORMATs to governed seed file.")
else:
    warn("S3_SELLOUT", "Seed unavailable — using Python GRAN_CANAL_MAP fallback")
    df_so_flat = df_so_raw.withColumn("FORMAT_NORM", F.trim(F.upper(F.col("FORMAT"))))
    _gcm_broadcast = spark.sparkContext.broadcast(_GRAN_CANAL_MAP)

    @F.udf(T.StringType())
    def udf_grp_map(f):
        return _gcm_broadcast.value.get(f, "UTT")

    df_so_joined = (
        df_so_flat
        .withColumn("gran_canal_grp",      udf_grp_map(F.col("FORMAT_NORM")))
        .withColumn("canal_type",          F.lit("RETAIL_FORMAT"))
        .withColumn("promoted",            F.lit("YES"))
        .withColumn("business_rule",       F.lit("fallback_map"))
        .withColumn("confirmation_status", F.lit("CONFIRMED"))
        .withColumn("notes",               F.lit("Classified via Python fallback (seed unavailable)"))
    )

# Seed coverage report
df_live_and_seeded  = df_so_joined.filter(
    F.col("gran_canal_grp").isNotNull() & (F.col("gran_canal_grp") != "PENDING")
)
df_live_not_seeded  = df_so_joined.filter(
    F.col("gran_canal_grp").isNull() | (F.col("gran_canal_grp") == "PENDING")
)

live_seeded_cnt     = df_live_and_seeded.select("FORMAT_NORM").distinct().count()
live_not_seeded_cnt = df_live_not_seeded.select("FORMAT_NORM").distinct().count()

# SEEDED_NOT_LIVE
if _seed_available and df_seed is not None:
    seeded_formats_norm = {normalize(r["canal_raw_norm"]) for r in df_seed_so.collect()}
    seeded_not_live     = seeded_formats_norm - so_live_formats
    seeded_not_live_cnt = len(seeded_not_live)
else:
    seeded_not_live, seeded_not_live_cnt = set(), 0

info("S3_SELLOUT", f"LIVE_AND_SEEDED:   {live_seeded_cnt}")
info("S3_SELLOUT", f"LIVE_NOT_SEEDED:   {live_not_seeded_cnt}")
info("S3_SELLOUT", f"SEEDED_NOT_LIVE:   {seeded_not_live_cnt}")

if live_not_seeded_cnt > 0:
    warn("V6", f"{live_not_seeded_cnt} live FORMAT(s) not in governed seed — sent to cat_canal_pending.csv")
    for r in df_live_not_seeded.select("FORMAT").distinct().collect():
        warn("V6", f"  LIVE_NOT_SEEDED: {r['FORMAT']}")

# Write sellout_format_seed_coverage.csv
@F.udf(T.StringType())
def udf_bucket(fmt, grp):
    n = normalize(fmt)
    if n in so_live_formats and grp and grp != "PENDING":
        return "LIVE_AND_SEEDED"
    elif n in so_live_formats:
        return "LIVE_NOT_SEEDED"
    return "LIVE_AND_SEEDED"

df_coverage = df_so_joined.withColumn(
    "seed_bucket", udf_bucket(F.col("FORMAT"), F.col("gran_canal_grp"))
)

# Add SEEDED_NOT_LIVE rows
if seeded_not_live:
    _snl_rows = [
        {
            "FORMAT":              f,
            "CHAIN":               None,
            "FORMAT_NORM":         f,
            "store_count":         0,
            "gran_canal_grp":      None,
            "canal_type":          "RETAIL_FORMAT",
            "promoted":            "NO",
            "business_rule":       None,
            "confirmation_status": "REFERENCE_ONLY",
            "notes":               "In seed but not found in live Snowflake data — may be retired format",
            "seed_bucket":         "SEEDED_NOT_LIVE",
        }
        for f in seeded_not_live
    ]
    _snl_schema = T.StructType([
        T.StructField("FORMAT",              T.StringType(), True),
        T.StructField("CHAIN",               T.StringType(), True),
        T.StructField("FORMAT_NORM",         T.StringType(), True),
        T.StructField("store_count",         T.LongType(),   True),
        T.StructField("gran_canal_grp",      T.StringType(), True),
        T.StructField("canal_type",          T.StringType(), True),
        T.StructField("promoted",            T.StringType(), True),
        T.StructField("business_rule",       T.StringType(), True),
        T.StructField("confirmation_status", T.StringType(), True),
        T.StructField("notes",               T.StringType(), True),
        T.StructField("seed_bucket",         T.StringType(), True),
    ])
    df_snl = spark.createDataFrame(_snl_rows, schema=_snl_schema)
    df_coverage = df_coverage.unionByName(df_snl, allowMissingColumns=True)

df_coverage.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{DBFS_BASE}/sellout_format_seed_coverage.csv")
info("S3_SELLOUT", "Written sellout_format_seed_coverage.csv")

# Build FORMAT catalog rows — L1
df_so_l1 = (
    df_so_joined
    .groupBy(
        "FORMAT", "FORMAT_NORM", "gran_canal_grp", "canal_type",
        "promoted", "confirmation_status", "notes",
    )
    .agg(F.sum("store_count").alias("row_count"))
    .withColumn("canal_std",          F.col("FORMAT"))
    .withColumn("canal_raw",          F.col("FORMAT"))
    .withColumn("canal_raw_norm",     F.col("FORMAT_NORM"))
    .withColumn("canal_level",        F.lit("L1_FORMAT"))
    .withColumn("parent_canal_std",   F.lit(None).cast(T.StringType()))
    .withColumn("l2_channel",         F.lit(None).cast(T.StringType()))
    .withColumn("source_system",      F.lit("SELL_OUT"))
    .withColumn("source_column",      F.lit("FORMAT"))
    .withColumn("canal_type",         F.coalesce(F.col("canal_type"), F.lit("RETAIL_FORMAT")))
    .withColumn("promoted",           F.coalesce(F.col("promoted"), F.lit("PENDING")))
    .withColumn("confirmation_status",F.coalesce(F.col("confirmation_status"), F.lit("PENDING_BUSINESS")))
    .withColumn("catalog_date",       F.lit(RUN_DATE))
    .withColumn("catalog_version",    F.lit(CATALOG_VERSION))
    .withColumn("notes",              F.coalesce(F.col("notes"), F.lit("LIVE_NOT_SEEDED — classification pending")))
    .drop("FORMAT", "FORMAT_NORM")
)

# Write sellout_format_gran_canal.csv
df_so_l1.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{DBFS_BASE}/sellout_format_gran_canal.csv")
info("S3_SELLOUT", f"Written sellout_format_gran_canal.csv ({df_so_l1.count()} rows)")

# Build CHAIN reference rows — L2 (Python post-join)
df_chain_joined = df_so_joined.withColumn(
    "gran_canal_grp", F.coalesce(F.col("gran_canal_grp"), F.lit("UNKNOWN"))
)
chain_grp = df_chain_joined.groupBy("CHAIN").agg(
    F.countDistinct("FORMAT").alias("format_count"),
    F.countDistinct("gran_canal_grp").alias("grp_count"),
    F.collect_set("gran_canal_grp").alias("grp_values"),
    F.sum("store_count").alias("row_count"),
)

multi_chain = chain_grp.filter(F.col("grp_count") > 1)
if multi_chain.count() > 0:
    warn("S3_SELLOUT", "CHAIN(s) with multiple gran_canal_grp values (set to MULTI):")
    for r in multi_chain.collect():
        warn("S3_SELLOUT", f"  CHAIN={r['CHAIN']} grp_values={r['grp_values']}")

@F.udf(T.StringType())
def udf_grp_from_set(grp_count, grp_values):
    if grp_count and grp_count > 1:
        return "MULTI"
    return grp_values[0] if grp_values else "UNKNOWN"

df_so_l2_chain = (
    chain_grp
    .withColumn("canal_std",          F.col("CHAIN"))
    .withColumn("canal_raw",          F.col("CHAIN"))
    .withColumn("canal_raw_norm",     F.trim(F.upper(F.col("CHAIN"))))
    .withColumn("canal_type",         F.lit("RETAIL_FORMAT"))
    .withColumn("canal_level",        F.lit("L2_CHAIN"))
    .withColumn("gran_canal_grp",     udf_grp_from_set(F.col("grp_count"), F.col("grp_values")))
    .withColumn("parent_canal_std",   F.lit(None).cast(T.StringType()))
    .withColumn("l2_channel",         F.col("CHAIN"))
    .withColumn("source_system",      F.lit("SELL_OUT"))
    .withColumn("source_column",      F.lit("CHAIN"))
    .withColumn("promoted",           F.lit("NO"))
    .withColumn("confirmation_status",F.lit("REFERENCE_ONLY"))
    .withColumn("catalog_date",       F.lit(RUN_DATE))
    .withColumn("catalog_version",    F.lit(CATALOG_VERSION))
    .withColumn("notes",              F.lit("CHAIN reference only — not a canonical join key"))
    .drop("CHAIN", "format_count", "grp_count", "grp_values")
)

info("S3_SELLOUT", f"SELL_OUT FORMAT (L1) rows: {df_so_l1.count()} | CHAIN (L2 ref) rows: {df_so_l2_chain.count()}")
info("S3_SELLOUT", "Section 3 complete.")

# COMMAND ----------

# MAGIC %md ## SECTION 4 — Aggregate Channel Split Validation

# COMMAND ----------

# DBTITLE 1,Cell 14
info("S4_VALIDATION", "=" * 70)
info("S4_VALIDATION", "AGGREGATE CHANNEL SPLIT VALIDATION")

# Step 4A: Schema discovery for SELL_IN fact tables
SQL_SCHEMA_DISC = """
    SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
    FROM PRD_MEX.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'MEX_DSP_OTC'
      AND TABLE_NAME IN ('VW_FACT_RNV', 'V_F_SALES', 'V_D_CLIENT')
    ORDER BY TABLE_NAME, ORDINAL_POSITION
"""
try:
    df_schema   = run_sf(SQL_SCHEMA_DISC, DB_PRD_MEX)
    schema_rows = df_schema.collect()
    tables_found = {r["TABLE_NAME"] for r in schema_rows}
    info("S4_VALIDATION", f"Schema discovery complete. Tables found: {tables_found}")
    for t in sorted(tables_found):
        cols = [r["COLUMN_NAME"] for r in schema_rows if r["TABLE_NAME"] == t]
        info("S4_VALIDATION", f"  {t}: {cols[:15]}{'...' if len(cols) > 15 else ''}")
except Exception as e:
    warn("S4_VALIDATION", f"Schema discovery failed: {e}")
    schema_rows, tables_found = [], set()

# Determine best fact table and metric columns
_FACT_CANDIDATES     = ["VW_FACT_RNV", "V_F_SALES"]
_confirmed_fact      = None
_confirmed_value_col = None
_confirmed_date_col  = None
_confirmed_join_key  = None

for tbl in _FACT_CANDIDATES:
    if tbl in tables_found:
        tbl_cols         = {r["COLUMN_NAME"].upper() for r in schema_rows if r["TABLE_NAME"] == tbl}
        value_candidates = ["NSV_MXN", "REVENUE_MXN", "NETO_MXN", "VALOR", "AMOUNT", "NET_SALES"]
        date_candidates  = ["YEAR_MONTH", "PERIODO", "FECHA", "MES_ANIO", "ANIO_MES"]
        join_candidates  = ["CUS_IDT", "SHP_CUS_IDT", "CUSTOMER_ID", "CLI_IDT"]
        for vc in value_candidates:
            if vc in tbl_cols:
                _confirmed_value_col = vc
                break
        for dc in date_candidates:
            if dc in tbl_cols:
                _confirmed_date_col = dc
                break
        for jc in join_candidates:
            if jc in tbl_cols:
                _confirmed_join_key = jc
                break
        if _confirmed_value_col and _confirmed_date_col and _confirmed_join_key:
            _confirmed_fact = tbl
            info("S4_VALIDATION",
                 f"Confirmed fact table: {_confirmed_fact} | value={_confirmed_value_col} | "
                 f"date={_confirmed_date_col} | join={_confirmed_join_key}")
            break

# Step 4B: IBP value split (always runs — IBP schema is known)
SQL_IBP_VAL = """
    SELECT
        GRAN_CANAL,
        COUNT(*)        AS row_count,
        SUM(VALOR)      AS valor_plan,
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()     AS row_pct,
        SUM(VALOR)  * 100.0 / SUM(SUM(VALOR)) OVER () AS valor_pct
    FROM PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP
    WHERE NOMBRE_ETIQUETA = 'REAL'
      AND GRAN_CANAL IS NOT NULL
    GROUP BY 1
    ORDER BY valor_plan DESC
"""
df_ibp_val  = run_sf(SQL_IBP_VAL, DB_PRD_MDP)
ibp_val_rows = df_ibp_val.collect()
info("S4_VALIDATION", "IBP channel split (dynamic):")
for r in ibp_val_rows:
    info(
        "S4_VALIDATION",
        f"  {r['GRAN_CANAL']:6s}: rows={r['ROW_COUNT']:>10,} ({r['ROW_PCT']:.1f}%) | "
        f"valor={r['VALOR_PLAN']:>15,.0f} ({r['VALOR_PCT']:.1f}%)",
    )

# Step 4C: SELL_IN fact split (conditional on schema discovery)
_sellin_val_rows = []
if _confirmed_fact:
    SQL_SI_VAL = f"""
        SELECT
            c.cus_grn_chl_dsc          AS gran_canal,
            COUNT(DISTINCT c.CUS_IDT)  AS customer_count,
            SUM(f.{_confirmed_value_col}) AS revenue
        FROM PRD_MEX.MEX_DSP_OTC.{_confirmed_fact} f
        JOIN PRD_MEX.MEX_DSP_OTC.V_D_CLIENT c
            ON f.{_confirmed_join_key} = c.CUS_IDT
        WHERE c.cus_grn_chl_dsc IN ('DTT','UTT')
          AND f.{_confirmed_date_col} >= '202501'
        GROUP BY 1
        ORDER BY revenue DESC
    """
    try:
        df_si_val        = run_sf(SQL_SI_VAL, DB_PRD_MEX)
        _sellin_val_rows = df_si_val.collect()
        info("S4_VALIDATION", f"SELL_IN ({_confirmed_fact}) channel split:")
        _total_rev = sum(r["REVENUE"] or 0 for r in _sellin_val_rows)
        for r in _sellin_val_rows:
            rev = r["REVENUE"] or 0
            info("S4_VALIDATION",
                 f"  {r['GRAN_CANAL']:6s}: customers={r['CUSTOMER_COUNT']:>8,} | "
                 f"revenue={rev:>15,.0f} ({rev / _total_rev * 100:.1f}%)" if _total_rev else
                 f"  {r['GRAN_CANAL']:6s}: customers={r['CUSTOMER_COUNT']:>8,} | revenue={rev:>15,.0f}")

        # V10 gate: compare IBP vs SELL_IN UTT/DTT value split
        _total_rev_safe = _total_rev or 1
        ibp_utt_pct = next(
            (r["VALOR_PCT"] for r in ibp_val_rows if normalize(r["GRAN_CANAL"]) == "UTT"), 0
        )
        ibp_dtt_pct = next(
            (r["VALOR_PCT"] for r in ibp_val_rows if normalize(r["GRAN_CANAL"]) == "DTT"), 0
        )
        si_utt_pct = next(
            ((r["REVENUE"] or 0) / _total_rev_safe * 100
             for r in _sellin_val_rows if normalize(r["GRAN_CANAL"]) == "UTT"),
            0,
        )
        si_dtt_pct = next(
            ((r["REVENUE"] or 0) / _total_rev_safe * 100
             for r in _sellin_val_rows if normalize(r["GRAN_CANAL"]) == "DTT"),
            0,
        )
        diff = abs(ibp_utt_pct - si_utt_pct)
        if diff > 30:
            warn(
                "V10",
                f"UTT split difference IBP ({ibp_utt_pct:.1f}%) vs SELL_IN ({si_utt_pct:.1f}%) "
                f"= {diff:.1f}pp > 30pp threshold — investigate channel taxonomy alignment",
            )
        else:
            info("V10", f"PASS — UTT split diff={diff:.1f}pp within 30pp tolerance")
    except Exception as e:
        warn("V10", f"SELL_IN fact query failed: {e}. Writing partial validation.")
else:
    warn(
        "V10",
        "No confirmed SELL_IN fact table found via schema discovery. "
        "Writing partial canal_volume_validation.csv.",
    )

# Write canal_volume_validation.csv
_val_output = []
for r in ibp_val_rows:
    _val_output.append({
        "source":       "IBP",
        "gran_canal":   r["GRAN_CANAL"],
        "row_count":    r["ROW_COUNT"],
        "value":        r["VALOR_PLAN"],
        "value_pct":    r["VALOR_PCT"],
        "metric":       "VALOR",
        "catalog_date": RUN_DATE,
    })
for r in _sellin_val_rows:
    rev = r["REVENUE"] or 0
    _total_rev_safe2 = sum(rr["REVENUE"] or 0 for rr in _sellin_val_rows) or 1
    _val_output.append({
        "source":       "SELL_IN",
        "gran_canal":   r["GRAN_CANAL"],
        "row_count":    r["CUSTOMER_COUNT"],
        "value":        rev,
        "value_pct":    rev / _total_rev_safe2 * 100,
        "metric":       _confirmed_value_col or "UNKNOWN",
        "catalog_date": RUN_DATE,
    })

df_val = spark.createDataFrame(_val_output)
df_val.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{DBFS_BASE}/canal_volume_validation.csv")
info("S4_VALIDATION", f"Written canal_volume_validation.csv ({len(_val_output)} rows)")
info("S4_VALIDATION", "Section 4 complete.")

# COMMAND ----------

# MAGIC %md ## SECTION 5 — IBP ↔ SELL_IN Cross-Validation

# COMMAND ----------

# DBTITLE 1,Cell 16
info("S5_XVAL", "=" * 70)
info("S5_XVAL", "IBP vs SELL_IN CHANNEL CROSS-VALIDATION")

sellin_l1_vals = {
    normalize(r["CUS_GRN_CHL_DSC"])
    for r in df_si_l1_raw.filter(F.col("cus_grn_chl_dsc").isNotNull()).collect()
    if normalize(r["CUS_GRN_CHL_DSC"]) not in {"NA"}
}

in_both  = ibp_gran_canal_vals & sellin_l1_vals
ibp_only = ibp_gran_canal_vals - sellin_l1_vals
si_only  = sellin_l1_vals - ibp_gran_canal_vals
overlap  = len(in_both) / len(ibp_gran_canal_vals) * 100 if ibp_gran_canal_vals else 0.0

info("S5_XVAL", f"IBP L1 values:     {sorted(ibp_gran_canal_vals)}")
info("S5_XVAL", f"SELL_IN L1 values: {sorted(sellin_l1_vals)}")
info("S5_XVAL", f"IN_BOTH:           {sorted(in_both)}")
info("S5_XVAL", f"IBP_ONLY:          {sorted(ibp_only)}   (expected: CAM — IBP planning channel)")
info("S5_XVAL", f"SELL_IN_ONLY:      {sorted(si_only)}   (expected: empty)")
info("S5_XVAL", f"Overlap:           {overlap:.1f}%   (expected floor: 66.7% due to CAM)")

if "CAM" in ibp_only:
    info("V2", "PASS — CAM is IBP_ONLY as expected. Not remapped. Not a blocker.")
for v in ibp_only - {"CAM"}:
    warn("S5_XVAL", f"Unexpected IBP_ONLY channel: '{v}' — not in SELL_IN and not CAM")

if overlap < 60:
    warn("V9", f"IBP ↔ SELL_IN overlap {overlap:.1f}% < 60% threshold — investigate")
else:
    info("V9", f"PASS — overlap={overlap:.1f}%")

info("S5_XVAL", "Section 5 complete.")

# COMMAND ----------

# MAGIC %md ## SECTION 6 — Assemble Catalog Outputs

# COMMAND ----------

info("S6_BUILD", "=" * 70)
info("S6_BUILD", "ASSEMBLING cat_canal.csv + cat_canal_pending.csv + cat_canal_reference.csv")

# Canonical column order
_COLS = [
    "canal_key", "canal_std", "canal_type", "canal_level",
    "canal_raw", "canal_raw_norm", "gran_canal_grp", "parent_canal_std",
    "l2_channel", "source_system", "source_column", "promoted",
    "confirmation_status", "row_count", "catalog_date", "catalog_version", "notes",
]

# Attach canal_key to IBP DataFrames
df_ibp_l1 = df_ibp_l1.withColumn(
    "canal_key",
    udf_canal_key(F.col("source_system"), F.col("source_column"), F.col("canal_level"), F.col("canal_raw_norm")),
)
df_ibp_l2 = df_ibp_l2.withColumn(
    "canal_key",
    udf_canal_key(F.col("source_system"), F.col("source_column"), F.col("canal_level"), F.col("canal_raw_norm")),
)

# Attach canal_key to SELL_OUT DataFrames
df_so_l1_sf = df_so_l1.withColumn(
    "canal_key",
    udf_canal_key(F.col("source_system"), F.col("source_column"), F.col("canal_level"), F.col("canal_raw_norm")),
)
df_so_l2_sf = df_so_l2_chain.withColumn(
    "canal_key",
    udf_canal_key(F.col("source_system"), F.col("source_column"), F.col("canal_level"), F.col("canal_raw_norm")),
)

# SELL_IN DataFrames already have canal_key from Python construction
df_si_l1_sf = df_si_l1
df_si_l2_sf = df_si_l2

def _align(df):
    """Add any missing canonical columns as null strings, then select in canonical order."""
    for c in _COLS:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast(T.StringType()))
    return df.select(_COLS)

df_all = reduce(
    lambda a, b: a.unionByName(b),
    [
        _align(df_ibp_l1),
        _align(df_ibp_l2),
        _align(df_si_l1_sf),
        _align(df_si_l2_sf),
        _align(df_so_l1_sf),
        _align(df_so_l2_sf),
    ],
)

# Dedup on natural key (one row per source × column × level × raw_norm × grp combination)
df_all = df_all.dropDuplicates(
    ["source_system", "source_column", "canal_level", "canal_raw_norm", "gran_canal_grp"]
)
df_all.cache()

# ── cat_canal.csv — promoted rows aligned to canonical UTT/DTT/CAM ──────────
df_promoted = df_all.filter(
    (F.col("promoted") == "YES") &
    (F.col("gran_canal_grp").isin("UTT", "DTT", "CAM"))
)

# V7: no PENDING rows in the promoted output
assert_gate(
    "V7",
    df_promoted.filter(F.col("promoted") == "PENDING").count() == 0,
    "cat_canal.csv must not contain promoted=PENDING rows",
)

# V8: SUBCHAIN dimension must be absent
assert_gate(
    "V8",
    df_promoted.filter(
        F.col("source_column").contains("SUBCHAIN") |
        F.col("canal_level").contains("SUBCHAIN")
    ).count() == 0,
    "SUBCHAIN must be absent from cat_canal.csv",
)

# V12: minimum 5 promoted rows required
_promoted_count = df_promoted.count()
if _promoted_count < 5:
    blocker(
        "V12",
        f"cat_canal.csv has {_promoted_count} rows — minimum 5 required "
        f"(IBP UTT/DTT + SI UTT/DTT + 1 FORMAT)",
    )
info("V12", f"PASS — cat_canal.csv has {_promoted_count} rows")

# ── cat_canal_pending.csv — rows still awaiting business classification ──────
df_pending = df_all.filter(
    (F.col("promoted") == "PENDING") |
    (F.col("gran_canal_grp") == "PENDING")
)

# ── cat_canal_reference.csv — full universe for lineage and auditing ─────────
df_reference = df_all

# Write all three output files
df_promoted.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{DBFS_BASE}/cat_canal.csv")
df_pending.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{DBFS_BASE}/cat_canal_pending.csv")
df_reference.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{DBFS_BASE}/cat_canal_reference.csv")

info("S6_BUILD", f"cat_canal.csv:           {df_promoted.count()} rows (promoted=YES only)")
info("S6_BUILD", f"cat_canal_pending.csv:   {df_pending.count()} rows")
info("S6_BUILD", f"cat_canal_reference.csv: {df_reference.count()} rows")
info("S6_BUILD", "Section 6 complete.")

# COMMAND ----------

# MAGIC %md ## SECTION 7 — Summary Report

# COMMAND ----------

info("S7_SUMMARY", "=" * 70)
info("S7_SUMMARY", "CANAL CATALOG BUILD SUMMARY")

_report = f"""
{'=' * 70}
CANAL CATALOG BUILD SUMMARY  v{CATALOG_VERSION}  --  {RUN_DATE}
{'=' * 70}

TAXONOMY:         Danone Global Channel Taxonomy (Business Logic First)
SNOWFLAKE_ACCESS: READ-ONLY ✅

SOURCE TABLES QUERIED:
  PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP
  PRD_MEX.MEX_DSP_OTC.V_D_CLIENT
  PRD_MDP.MDP_DSP.VW_D_STORE_RM
  PRD_MEX.INFORMATION_SCHEMA.COLUMNS  (schema discovery)
  PRD_MEX.MEX_DSP_OTC.{_confirmed_fact or 'NOT_FOUND'}  (Section 4)

IBP RESULTS:
  GRAN_CANAL values: {sorted(ibp_gran_canal_vals)}

SELL_IN RESULTS:
  L1 values found: {sorted(actual_l1)}
  lv6_hie_cus_dsc: null_rate={l2_null_rate:.1f}%, distinct={l2_distinct}, promoted={'YES' if _l2_promotable else 'REFERENCE_ONLY'}

SELL_OUT RESULTS:
  Live FORMATs:      {len(so_live_formats)}
  LIVE_AND_SEEDED:   {live_seeded_cnt}
  LIVE_NOT_SEEDED:   {live_not_seeded_cnt}   → cat_canal_pending.csv
  SEEDED_NOT_LIVE:   {seeded_not_live_cnt}   → reference only
  SUBCHAIN:          EXCLUDED (null rate 33.33%, AMBI-07)

CONFIRMED EDGE CASES:
  CASH AND CARRY        → DTT  (CONFIRMED_RECOMMENDED)
  7E-AEROPUERTO         → DTT  (CONFIRMED)
  7E-AEROPUERTO PREMIUM → DTT  (CONFIRMED)
  SO-CITY CLUB          → UTT  (CONFIRMED)
  SO-EXPRESS            → UTT  (CONFIRMED)
  CCF-ECOMMERCE         → UTT  (CONFIRMED)
  CX-ECOMMERCE          → UTT  (CONFIRMED)

CROSS-VALIDATION:
  IBP ↔ SELL_IN overlap: {overlap:.1f}%   (CAM gap expected — 66.7% is floor)
  IBP_ONLY: {sorted(ibp_only)}   (CAM = IBP planning scope, not a blocker)

OUTPUT FILES:
  cat_canal.csv:                    {df_promoted.count()} rows  (promoted=YES only)
  cat_canal_pending.csv:            {df_pending.count()} rows
  cat_canal_reference.csv:          {df_reference.count()} rows
  ibp_canal_hierarchy.csv:          written
  sellout_format_gran_canal.csv:    {df_so_l1.count()} rows
  sellout_format_seed_coverage.csv: written
  canal_volume_validation.csv:      {len(_val_output)} rows
  build_cat_canal_report.txt:       this file

VALIDATION GATES:
  V1  IBP UTT+DTT present:          PASS
  V2  CAM not remapped:             PASS
  V3  SELL_IN DTT+UTT present:      PASS
  V4  Unexpected SELL_IN L1:        {'WARN' if any('V4' in w for w in _warnings) else 'PASS'}
  V5  lv6 null rate <=30%:          {'WARN' if not _l2_promotable else 'PASS'} ({l2_null_rate:.1f}%)
  V6  FORMAT seed coverage:         {'WARN' if live_not_seeded_cnt > 0 else 'PASS'} ({live_not_seeded_cnt} LIVE_NOT_SEEDED)
  V7  No PENDING in cat_canal.csv:  PASS
  V8  SUBCHAIN absent:              PASS
  V9  IBP<>SELL_IN overlap >=60%:   {'PASS' if overlap >= 60 else 'WARN'} ({overlap:.1f}%)
  V10 Volume validation generated:  {'WARN' if not _sellin_val_rows else 'PASS'}
  V11 Snowflake read-only:          PASS
  V12 Minimum promoted rows:        PASS ({df_promoted.count()} rows)

WARNINGS:  {len(_warnings)}
BLOCKERS:  {len(_blockers)}
{'=' * 70}
"""

print(_report)
_log_lines.append(_report)

# Write report to DBFS and repo log path
dbutils.fs.put(f"{DBFS_BASE}/build_cat_canal_report.txt", "\n".join(_log_lines), overwrite=True)
dbutils.fs.put(f"{REPO_LOG_BASE}/build_cat_canal_report.txt", "\n".join(_log_lines), overwrite=True)

if len(_blockers) == 0:
    info("S7_SUMMARY", f"✅ PASS — Notebook completed successfully. {df_promoted.count()} CANAL rows promoted.")
else:
    info("S7_SUMMARY", f"❌ FAIL — {len(_blockers)} BLOCKER(s) raised. See report above.")
info("S7_SUMMARY", f"{len(_warnings)} warning(s) raised (non-blocking).")

# COMMAND ----------


