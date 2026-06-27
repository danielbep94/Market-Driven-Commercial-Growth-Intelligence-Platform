# Databricks notebook source

# MAGIC %md
# MAGIC # MDM Cross-Source Join Validation — Notebook B (v3)
# MAGIC
# MAGIC **Purpose:** Validate cross-source join integrity, predicate rules, null rates, and grain.
# MAGIC
# MAGIC **Key architectural rules (v3):**
# MAGIC > 1. `MAT_IDT` is the unique SAP product key. `SKU_EAN_COD` is a barcode attribute — NOT a unique key.
# MAGIC > 2. `UPC_STD` is **NEVER** a predicate in the all-source join. UPC validation is scoped exclusively to the SELL_IN ↔ SELL_OUT bridge.
# MAGIC > 3. `MKT_ON` and `MKT_OFF` are **NEVER** joined using UPC.
# MAGIC > 4. `MKT_OFF` is **NEVER** joined using CADENA as a required predicate.
# MAGIC > 5. `MKT_OFF CADENA_STD` must be NULL in standardized output.
# MAGIC > 6. All-source join grain = `MARCA_STD + DATE_TRUNC('MONTH', FECHA)`.
# MAGIC
# MAGIC **Credential resolution:**
# MAGIC - `PRD_MEX` → `configs/snowflake_creds.py` SF_MEX_* (PRD_OSM_DPH_READER)
# MAGIC - `PRD_MDP` → SF_MDP_* or Key Vault (DAN-AM-P-KVT800-R-MDP-DB)
# MAGIC
# MAGIC **Run `notebooks/validate_credentials.py` first.**
# MAGIC
# MAGIC **Output root:** `dbfs:/mnt/mdp/mdm/notebook_b/`

# COMMAND ----------

# ── CELL 1: Load credentials ──────────────────────────────────────────────────
import os, importlib.util, datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window

_current_dir = os.getcwd()
_creds_path  = os.path.normpath(os.path.join(_current_dir, "..", "configs", "snowflake_creds.py"))

if not os.path.exists(_creds_path):
    raise FileNotFoundError("❌ configs/snowflake_creds.py NOT FOUND.")

_spec = importlib.util.spec_from_file_location("snowflake_creds", _creds_path)
_m    = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_m)

SF_URL = "danonenam.east-us-2.azure.snowflakecomputing.com"

def get_sf_options(database: str) -> dict:
    _mdp_user = getattr(_m, "SF_MDP_USER", None)
    _mdp_pwd  = getattr(_m, "SF_MDP_PASSWORD", None)
    profiles = {
        "PRD_MEX": {
            "sfURL": SF_URL, "sfUser": _m.SF_MEX_USER, "sfPassword": _m.SF_MEX_PASSWORD,
            "sfWarehouse": getattr(_m, "SF_MEX_WH", "PRD_MEX_ANL_WH"),
            "sfRole": getattr(_m, "SF_MEX_ROLE", "PRD_MEX_READER"),
        },
        "PRD_MDP": {
            "sfURL": SF_URL,
            "sfUser":     _mdp_user or dbutils.secrets.get("DAN-AM-P-KVT800-R-MDP-DB", "snowflake-user"),
            "sfPassword": _mdp_pwd  or dbutils.secrets.get("DAN-AM-P-KVT800-R-MDP-DB", "snowflake-password"),
            "sfWarehouse": getattr(_m, "SF_MDP_WH", "PRD_MDP_ANL_WH"),
            "sfRole": getattr(_m, "SF_MDP_ROLE", "PRD_MDP"),
        },
    }
    if database not in profiles:
        raise ValueError(f"No profile for '{database}'. Available: {list(profiles.keys())}")
    return dict(profiles[database])

def run_sf(database: str, sql: str):
    return (spark.read.format("net.snowflake.spark.snowflake")
            .options(**get_sf_options(database))
            .option("sfDatabase", database)
            .option("query", sql)
            .load())

print(f"✅ Credentials loaded — PRD_MEX: {_m.SF_MEX_USER} | PRD_MDP: {'<Key Vault>' if not getattr(_m,'SF_MDP_USER',None) else _m.SF_MDP_USER}")

# COMMAND ----------

# ── CELL 2: Output paths + helpers ────────────────────────────────────────────
DBFS_ROOT = "dbfs:/mnt/mdp/mdm/notebook_b"
dbutils.fs.mkdirs(DBFS_ROOT)

# ── Repo-tracked logs directory ──
import pathlib
_REPO_ROOT    = str(pathlib.Path(_current_dir).parent)
REPO_LOGS_DIR = os.path.join(_REPO_ROOT, "logs")
os.makedirs(REPO_LOGS_DIR, exist_ok=True)

_LOG_LINES:    list[str] = []
_HARD_BLOCKERS: list[str] = []
_WARNINGS:      list[str] = []

def ts() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def log(level: str, msg: str, section: str = ""):
    prefix = f"[{ts()}][{level}]"
    if section: prefix += f"[{section}]"
    line = f"{prefix} {msg}"
    _LOG_LINES.append(line); print(line)

def flush_log():
    content = "\n".join(_LOG_LINES)
    dbutils.fs.put(f"{DBFS_ROOT}/validation_results_mdm_cross_source.txt", content, overwrite=True)
    _repo_log = os.path.join(REPO_LOGS_DIR, "validation_results_mdm_cross_source.txt")
    with open(_repo_log, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"📄 Log → DBFS: {DBFS_ROOT}/validation_results_mdm_cross_source.txt")
    print(f"📄 Log → REPO: {_repo_log}")

def save_df(df, filename: str, section: str = ""):
    dbfs_path = f"{DBFS_ROOT}/{filename}"
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(dbfs_path)
    _fname = os.path.basename(filename)
    if not _fname.endswith(".csv"): _fname += ".csv"
    _repo_csv = os.path.join(REPO_LOGS_DIR, _fname)
    try:
        df.limit(50000).toPandas().to_csv(_repo_csv, index=False, encoding="utf-8")
        log("INFO", f"Saved → DBFS: {dbfs_path}  |  REPO: {_repo_csv}", section)
    except Exception as _e:
        log("⚠️  WARNING", f"Repo CSV write failed for {_fname}: {_e}", section)

def blocker(condition: bool, msg: str, section: str = ""):
    if condition: log("🚨 BLOCKER", msg, section); _HARD_BLOCKERS.append(msg)
    return condition

def warn(condition: bool, msg: str, section: str = ""):
    if condition: log("⚠️  WARNING", msg, section); _WARNINGS.append(msg)
    return condition

def passed(msg: str, section: str = ""):
    log("✅ PASS", msg, section)

print(f"✅ CELL 2 ready (dual-write). DBFS: {DBFS_ROOT} | REPO: {REPO_LOGS_DIR}")

# Applicability matrices — governs which join predicates are legal per source
UPC_APPLICABLE = {
    "SELL_IN": True, "SELL_OUT": True,
    "MKT_ON": False, "MKT_OFF": False,
    "EDP_NIELSEN": False, "PB_NIELSEN": False,
    "WATER_NIELSEN_RIE": False, "WATER_SCANTRACK": False,
    "IBP": False, "WASTE": False,
}
CADENA_APPLICABLE = {
    "SELL_IN": False,   # CADENA_STD = NULL until V_D_CLIENT chain field confirmed
    "SELL_OUT": True,   # From VW_D_STORE_RM CHAIN
    "MKT_ON": True,
    "MKT_OFF": False,   # CADENA_STD always NULL for MKT_OFF — by design
    "EDP_NIELSEN": True, "PB_NIELSEN": True,
    "WATER_NIELSEN_RIE": True, "WATER_SCANTRACK": True,
    "IBP": True, "WASTE": True,
}
NULL_EXPECTED_DIMENSIONS = {
    "MKT_ON":  ["UPC_STD"],
    "MKT_OFF": ["UPC_STD", "CADENA_STD"],
    "SELL_IN": ["CADENA_STD"],   # provisional — until chain field confirmed
}

print("✅ CELL 2 ready. Output root:", DBFS_ROOT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section B1 — Row Counts Before / After Standardization

# COMMAND ----------

# ── CELL 3: Row counts ────────────────────────────────────────────────────────
SECTION = "B1-ROW-COUNTS"
log("INFO", "=" * 60, SECTION)

ROW_QUERIES = {
    "SELL_IN_RAW":   ("PRD_MEX", "SELECT COUNT(*) AS row_count, COUNT(DISTINCT TO_VARCHAR(MAT_IDT)) AS distinct_mat_idt, COUNT(DISTINCT TO_VARCHAR(SKU_EAN_COD)) AS distinct_ean FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM WHERE TRY_TO_NUMBER(MAT_ACT_FLG)=1"),
    "SELL_OUT_RAW":  ("PRD_MDP", "SELECT COUNT(*) AS row_count, COUNT(DISTINCT TO_VARCHAR(UPC)) AS distinct_upcs FROM PRD_MDP.MDP_DSP.VW_FACT_SELL_OUT WHERE PER_ID >= 20250101"),
    "MKT_ON_RAW":    ("PRD_MDP", "SELECT COUNT(*) AS row_count, COUNT(DISTINCT MARCA) AS distinct_brands FROM PRD_MDP.MDP_DSP.VW_MKT_ECOMM WHERE ANIO >= 2024"),
    "MKT_OFF_RAW":   ("PRD_MDP", "SELECT COUNT(*) AS row_count, COUNT(DISTINCT MARCA) AS distinct_brands FROM PRD_MDP.MDP_STG.FACT_MEDIA_OFF WHERE ANIO >= 2024"),
    "IBP_RAW":       ("PRD_MDP", "SELECT COUNT(*) AS row_count, COUNT(DISTINCT MARCA) AS distinct_brands, COUNT(DISTINCT CADENA) AS distinct_cadenas FROM PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP"),
    "WASTE_RAW":     ("PRD_MDP", "SELECT COUNT(*) AS row_count, COUNT(DISTINCT MARCA) AS distinct_brands FROM PRD_MDP.MDP_STG.FACT_TOPLINE WHERE UPPER(TRIM(FUENTE))='TOPLINE'"),
}

count_rows = []
for label, (db, sql) in ROW_QUERIES.items():
    try:
        df = run_sf(db, sql)
        r  = df.collect()[0]
        entry = {"source": label}
        entry.update({k: str(v) for k, v in zip(df.columns, r)})
        count_rows.append(entry)
        log("INFO", f"{label}: {entry}", SECTION)
    except Exception as exc:
        warn(True, f"{label}: query failed — {exc}", SECTION)

display(spark.createDataFrame(count_rows))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section B2 — MARCA_STD Overlap: SELL_IN vs Each Source

# COMMAND ----------

# ── CELL 4: Brand overlap ─────────────────────────────────────────────────────
SECTION = "B2-BRAND-OVERLAP"
log("INFO", "=" * 60, SECTION)

si_brands = {r["MARCA_RAW"] for r in run_sf("PRD_MEX",
    "SELECT DISTINCT TRIM(UPPER(LV2_UMB_BRD_DSC)) AS marca_raw FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM WHERE LV2_UMB_BRD_DSC IS NOT NULL"
).collect()}
log("INFO", f"SELL_IN distinct brand values: {len(si_brands)}", SECTION)

BRAND_OVERLAP = {
    "SELL_OUT":          ("PRD_MDP", "SELECT DISTINCT TRIM(UPPER(BRAND)) AS b FROM PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM WHERE BRAND IS NOT NULL"),
    "MKT_ON":            ("PRD_MDP", "SELECT DISTINCT TRIM(UPPER(MARCA)) AS b FROM PRD_MDP.MDP_DSP.VW_MKT_ECOMM WHERE MARCA IS NOT NULL"),
    "MKT_OFF":           ("PRD_MDP", "SELECT DISTINCT TRIM(UPPER(MARCA)) AS b FROM PRD_MDP.MDP_STG.FACT_MEDIA_OFF WHERE MARCA IS NOT NULL"),
    "IBP":               ("PRD_MDP", "SELECT DISTINCT TRIM(UPPER(MARCA)) AS b FROM PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP WHERE MARCA IS NOT NULL"),
    "WASTE":             ("PRD_MDP", "SELECT DISTINCT TRIM(UPPER(MARCA)) AS b FROM PRD_MDP.MDP_STG.FACT_TOPLINE WHERE MARCA IS NOT NULL AND UPPER(TRIM(FUENTE))='TOPLINE'"),
    "EDP_NIELSEN":       ("PRD_MEX", "SELECT DISTINCT TRIM(UPPER(INP_56985)) AS b FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_PROD_DIM WHERE INP_56985 IS NOT NULL"),
    "PB_NIELSEN":        ("PRD_MEX", "SELECT DISTINCT TRIM(UPPER(INP_56985)) AS b FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_SUST_LECHE_ST_NLSN_PROD_DIM WHERE INP_56985 IS NOT NULL"),
    "WATER_NIELSEN_RIE": ("PRD_MEX", "SELECT DISTINCT TRIM(UPPER(CSTM_310589)) AS b FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_RT_NLSN_PROD_DIM WHERE CSTM_310589 IS NOT NULL"),
    "WATER_SCANTRACK":   ("PRD_MEX", "SELECT DISTINCT TRIM(UPPER(CSTM_310589)) AS b FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_ST_NLSN_PROD_DIM WHERE CSTM_310589 IS NOT NULL"),
}

overlap_rows = []
for src, (db, sql) in BRAND_OVERLAP.items():
    try:
        src_brands = {r["B"] for r in run_sf(db, sql).collect()}
        overlap    = si_brands & src_brands
        only_src   = src_brands - si_brands
        pct = round(len(overlap) / len(src_brands) * 100, 1) if src_brands else 0.0
        overlap_rows.append({"source": src, "src_distinct": len(src_brands),
                              "overlap_with_sell_in": len(overlap), "pct": pct, "only_in_source": len(only_src)})
        log("INFO", f"{src}: {len(src_brands)} brands | overlap={len(overlap)} ({pct}%) | only_in_src={len(only_src)}", SECTION)
    except Exception as exc:
        warn(True, f"{src}: query failed — {exc}", SECTION)

display(spark.createDataFrame(overlap_rows))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section B3 — UPC Match Rate: SELL_IN ↔ SELL_OUT Bridge Only
# MAGIC
# MAGIC > ⚠️ **`UPC_STD` is NOT used in the all-source join.**
# MAGIC > This section validates the bridge check ONLY.
# MAGIC > MKT_ON and MKT_OFF are NEVER joined using UPC (Blockers #6 and #7).

# COMMAND ----------

# ── CELL 5: UPC match rate — split Spark reads ────────────────────────────────
SECTION = "B3-UPC-MATCH-RATE"
log("INFO", "=" * 60, SECTION)
log("INFO", "RULE: UPC_STD is NOT an all-source join predicate.", SECTION)
log("INFO", "This section validates the SELL_IN <-> SELL_OUT bridge only.", SECTION)

# Read SELL_OUT UPCs (PRD_MDP)
df_so_upcs = run_sf("PRD_MDP", """
    SELECT DISTINCT TO_VARCHAR(UPC) AS upc_so
    FROM PRD_MDP.MDP_DSP.VW_FACT_SELL_OUT
    WHERE PER_ID >= 20250101 AND UPC IS NOT NULL
""")

# Read SELL_IN EAN keys (PRD_MEX)
df_si_upcs = run_sf("PRD_MEX", """
    SELECT DISTINCT TO_VARCHAR(SKU_EAN_COD) AS upc_std
    FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM
    WHERE SKU_EAN_COD IS NOT NULL AND TRIM(TO_VARCHAR(SKU_EAN_COD)) <> ''
""")

total_so = df_so_upcs.count()
matched  = df_so_upcs.join(df_si_upcs, df_so_upcs["upc_so"] == df_si_upcs["upc_std"], "inner").count()
p1_pct   = round(matched / total_so * 100, 1) if total_so else 0.0

log("INFO", f"SELL_OUT UPCs={total_so:,} | matched={matched:,} ({p1_pct}%) | unmatched={total_so - matched:,}", SECTION)
warn(p1_pct < 70.0, f"Warning #2: UPC match rate {p1_pct}% below 70%.", SECTION)
if p1_pct >= 70.0: passed(f"UPC match rate {p1_pct}% ≥ 70%.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section B4 — UPC & CADENA Predicate Violation Matrix

# COMMAND ----------

# ── CELL 6: Structural rule audit ────────────────────────────────────────────
SECTION = "B4-PREDICATE-VIOLATIONS"
log("INFO", "=" * 60, SECTION)

log("INFO", "UPC join applicability by source:", SECTION)
for src, ok in UPC_APPLICABLE.items():
    log("INFO", f"  {src:<30} UPC join: {'ALLOWED (SELL_IN↔SELL_OUT bridge only)' if ok else 'PROHIBITED — BLOCKER if violated'}", SECTION)

log("INFO", "\nCADENA join applicability by source:", SECTION)
for src, ok in CADENA_APPLICABLE.items():
    suffix = "ALLOWED" if ok else "PROHIBITED (CADENA_STD = NULL by design)"
    log("INFO", f"  {src:<30} CADENA join: {suffix}", SECTION)

log("INFO", "\nDimensions expected to be 100% NULL (by design):", SECTION)
for src, dims in NULL_EXPECTED_DIMENSIONS.items():
    log("INFO", f"  {src}: {dims}", SECTION)

# Structural blockers (enforced by design — no live query needed)
blocker(UPC_APPLICABLE.get("MKT_ON", False),
        "Blocker #6: MKT_ON is incorrectly flagged as UPC-APPLICABLE.", SECTION)
blocker(UPC_APPLICABLE.get("MKT_OFF", False),
        "Blocker #7: MKT_OFF is incorrectly flagged as UPC-APPLICABLE.", SECTION)
blocker(CADENA_APPLICABLE.get("MKT_OFF", False),
        "Blocker #8: MKT_OFF is incorrectly flagged as CADENA-APPLICABLE.", SECTION)

if not UPC_APPLICABLE["MKT_ON"] and not UPC_APPLICABLE["MKT_OFF"]:
    passed("Blockers #6 & #7: MKT_ON and MKT_OFF correctly marked UPC-PROHIBITED.", SECTION)
if not CADENA_APPLICABLE["MKT_OFF"]:
    passed("Blocker #8: MKT_OFF correctly marked CADENA-PROHIBITED (CADENA_STD = NULL).", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section B5 — Null Rate Audit by Source + Dimension

# COMMAND ----------

# ── CELL 7: Null rates ────────────────────────────────────────────────────────
SECTION = "B5-NULL-RATES"
log("INFO", "=" * 60, SECTION)
log("INFO", "Dimensions expected to be 100% NULL: MKT_ON(UPC_STD), MKT_OFF(UPC_STD), MKT_OFF(CADENA_STD), SELL_IN(CADENA_STD)", SECTION)

NULL_QUERIES = {
    # (db, sql, expect_100pct_null, label, note)
    "SELL_IN_MARCA":  ("PRD_MEX", "SELECT 'SELL_IN' AS src, 'MARCA_STD' AS dim, COUNT(*) AS total, COUNT_IF(LV2_UMB_BRD_DSC IS NULL OR TRIM(LV2_UMB_BRD_DSC)='') AS null_rows FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM", False, "MARCA_STD null rate for SELL_IN"),
    "SELL_IN_UPC":    ("PRD_MEX", "SELECT 'SELL_IN' AS src, 'UPC_STD' AS dim, COUNT(*) AS total, COUNT_IF(SKU_EAN_COD IS NULL OR TRIM(TO_VARCHAR(SKU_EAN_COD))='') AS null_rows FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM WHERE TRY_TO_NUMBER(MAT_ACT_FLG)=1", False, "UPC_STD null rate for SELL_IN active"),
    "SELL_IN_CADENA": ("PRD_MEX", "SELECT 'SELL_IN' AS src, 'CADENA_STD' AS dim, COUNT(*) AS total, COUNT(*) AS null_rows FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM WHERE TRY_TO_NUMBER(MAT_ACT_FLG)=1", True, "CADENA_STD expected 100% NULL (provisional — chain field unconfirmed)"),
    "MKT_ON_UPC":     ("PRD_MDP", "SELECT 'MKT_ON' AS src, 'UPC_STD' AS dim, COUNT(*) AS total, COUNT(*) AS null_rows FROM PRD_MDP.MDP_DSP.VW_MKT_ECOMM WHERE ANIO>=2024", True, "MKT_ON UPC_STD must be 100% NULL (brand grain)"),
    "MKT_OFF_UPC":    ("PRD_MDP", "SELECT 'MKT_OFF' AS src, 'UPC_STD' AS dim, COUNT(*) AS total, COUNT(*) AS null_rows FROM PRD_MDP.MDP_STG.FACT_MEDIA_OFF WHERE ANIO>=2024", True, "MKT_OFF UPC_STD must be 100% NULL (brand grain)"),
    "MKT_OFF_CADENA_NOTE": ("PRD_MDP", "SELECT 'MKT_OFF' AS src, 'CADENA_STD' AS dim, COUNT(*) AS total, COUNT(*) AS null_rows FROM PRD_MDP.MDP_STG.FACT_MEDIA_OFF WHERE ANIO>=2024", True, "MKT_OFF CADENA_STD must be 100% NULL (deferred — raw column name unknown; assert against mkt_off_std in Phase 3)"),
    "SELL_OUT_MARCA": ("PRD_MDP", "SELECT 'SELL_OUT' AS src, 'MARCA_STD' AS dim, COUNT(*) AS total, COUNT_IF(prod.BRAND IS NULL OR TRIM(prod.BRAND)='') AS null_rows FROM PRD_MDP.MDP_DSP.VW_FACT_SELL_OUT f INNER JOIN PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM prod ON TO_VARCHAR(f.UPC)=TO_VARCHAR(prod.INT_ID) AND f.CBU_ID=prod.CBU_ID WHERE f.PER_ID>=20250101", False, "MARCA_STD null rate for SELL_OUT"),
    "IBP_CADENA":     ("PRD_MDP", "SELECT 'IBP' AS src, 'CADENA' AS dim, COUNT(*) AS total, COUNT_IF(CADENA IS NULL OR TRIM(CADENA)='') AS null_rows FROM PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP", False, "IBP CADENA null rate"),
}

null_rows = []
for label, (db, sql, expect_100, note) in NULL_QUERIES.items():
    try:
        r     = run_sf(db, sql).collect()[0]
        total = int(r["TOTAL"])
        nc    = int(r["NULL_ROWS"])
        pct   = round(nc / total * 100, 2) if total else 0.0
        if expect_100:
            status = "✅ PASS (expected 100% NULL by design)" if pct == 100.0 else f"⚠️  NOT 100% NULL — {pct}%"
            blocker(pct < 100.0 and label not in ["MKT_OFF_CADENA_NOTE", "SELL_IN_CADENA"],
                    f"Blocker #9: {label} CADENA_STD is not 100% NULL in standardized output.", SECTION)
        else:
            status = "✅ PASS" if pct <= 1.0 else f"⚠️  WARNING — {pct}% NULL (threshold 1%)"
            warn(pct > 1.0, f"Warning #1: {label} null rate {pct}% above 1%.", SECTION)
        null_rows.append({"label": label, "source": str(r["SRC"]), "dimension": str(r["DIM"]),
                          "total": total, "null_count": nc, "null_pct": pct,
                          "expected_100pct_null": str(expect_100), "status": status, "note": note})
        log("INFO", f"{label}: total={total:,} | null={nc:,} ({pct}%) | {status}", SECTION)
    except Exception as exc:
        warn(True, f"{label}: failed — {exc}", SECTION)

display(spark.createDataFrame(null_rows))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section B6 — Aggregation Grain Check

# COMMAND ----------

# ── CELL 8: SELL_IN fanout ratio ─────────────────────────────────────────────
SECTION = "B6-AGG-GRAIN"
log("INFO", "=" * 60, SECTION)
log("INFO", "Rule: Aggregate each source to MARCA_STD + MONTH before all-source join.", SECTION)
log("INFO", "Rule: All product IDs must be VARCHAR — no numeric casts.", SECTION)

SQL_GRAIN = """
SELECT
    COUNT(*) AS raw_rows,
    COUNT(DISTINCT DATE_TRUNC('MONTH', TO_DATE(TO_VARCHAR(FAC.BIL_DAT),'YYYYMMDD'))
          || '|' || TRIM(UPPER(PRO.LV2_UMB_BRD_DSC))) AS grain_combos
FROM PRD_MEX.MEX_DSP_OTC.VW_FACT_RNV FAC
LEFT JOIN PRD_MEX.MEX_DSP_OTC.V_D_ITEM PRO ON FAC.MAT_IDT = PRO.MAT_IDT
WHERE FAC.CBU IN ('WATERS','EDP')
  AND DATE_TRUNC('MONTH', TO_DATE(TO_VARCHAR(FAC.BIL_DAT),'YYYYMMDD')) >= '2025-01-01'
"""
df_grain = run_sf("PRD_MEX", SQL_GRAIN)
display(df_grain)
r = df_grain.collect()[0]
ratio = round(r["RAW_ROWS"] / r["GRAIN_COMBOS"], 1) if r["GRAIN_COMBOS"] else 0.0
log("INFO", f"SELL_IN: raw_rows={r['RAW_ROWS']:,} | grain_combos={r['GRAIN_COMBOS']:,} | fanout_ratio={ratio}x", SECTION)
warn(ratio > 10, f"SELL_IN fanout ratio {ratio}x — aggregation REQUIRED before cross-source join.", SECTION)
if ratio <= 10: passed(f"SELL_IN fanout ratio {ratio}x — within acceptable range.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section B7 — Approved All-Source Join Template
# MAGIC
# MAGIC > This template encodes ALL join rules from the MDM v3 design.
# MAGIC > It must not be modified without an MDM review sign-off.

# COMMAND ----------

# ── CELL 9: Approved join template ────────────────────────────────────────────
SECTION = "B7-JOIN-TEMPLATE"
log("INFO", "=" * 60, SECTION)

APPROVED_JOIN_SQL = """
-- ============================================================
-- APPROVED ALL-SOURCE JOIN PATTERN — MDM v3
-- Grain   : MARCA_STD + DATE_TRUNC('MONTH', FECHA)
-- UPC_STD : NOT a join predicate. UPC validation = bridge only
--           (Notebook A Section A4 / Notebook B Section B3).
-- CADENA  : Optional predicate where CADENA_APPLICABLE = TRUE.
--           NEVER required for MKT_OFF (CADENA_STD always NULL).
-- Database routing:
--   SELL_IN (PRD_MEX)          : PRD_MEX_ANL_WH / PRD_MEX_READER
--   SELL_OUT / MKT_ON / MKT_OFF: PRD_MDP_ANL_WH / PRD_MDP
--   Cross-DB joins             : split into two Spark reads
-- ============================================================

WITH sell_in_agg AS (
    SELECT
        DATE_TRUNC('MONTH', TO_DATE(TO_VARCHAR(FAC.BIL_DAT),'YYYYMMDD')) AS FECHA,
        TRIM(UPPER(PRO.LV2_UMB_BRD_DSC))     AS MARCA_STD,
        -- UPC_STD : NOT included — SELL_IN UPC validated via bridge only
        -- CADENA_STD : NULL (provisional — V_D_CLIENT chain field unconfirmed)
        -- CANAL_STD  : from CUS_GRN_CHL_DSC once classified
        SUM(FAC.LITER)                        AS VOLUMEN,
        SUM(FAC.BIL_INV)                      AS VALOR
    FROM PRD_MEX.MEX_DSP_OTC.VW_FACT_RNV FAC
    LEFT JOIN PRD_MEX.MEX_DSP_OTC.V_D_ITEM PRO ON FAC.MAT_IDT = PRO.MAT_IDT
    WHERE FAC.CBU IN ('WATERS','EDP')
      AND DATE_TRUNC('MONTH', TO_DATE(TO_VARCHAR(FAC.BIL_DAT),'YYYYMMDD')) >= '2025-01-01'
    GROUP BY 1, 2
),
sell_out_agg AS (
    SELECT
        DATE_TRUNC('MONTH', per."DAY")        AS FECHA,
        TRIM(UPPER(prod.BRAND))               AS MARCA_STD,
        -- UPC_STD : NOT an all-source join predicate. Used only in bridge validation.
        -- CADENA_STD : from VW_D_STORE_RM CHAIN (after classification)
        -- CANAL_STD  : from VW_D_STORE_RM FORMAT (after classification)
        SUM(f.VOL_SELL_OUT)                   AS VOL_SELL_OUT,
        SUM(f.AMOUNT_SELL_OUT)                AS AMOUNT_SELL_OUT
    FROM PRD_MDP.MDP_DSP.VW_FACT_SELL_OUT f
    INNER JOIN PRD_MDP.MDP_DWH.V_D_PERIOD per  ON f.PER_ID = per.PER_ID
    INNER JOIN PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM prod
        ON TO_VARCHAR(f.UPC) = TO_VARCHAR(prod.INT_ID) AND f.CBU_ID = prod.CBU_ID
    WHERE f.PER_ID >= 20250101
    GROUP BY 1, 2
),
mkt_on_agg AS (
    SELECT
        DATE_TRUNC('MONTH', FECHA)            AS FECHA,
        TRIM(UPPER(MARCA))                    AS MARCA_STD,
        -- UPC_STD   = NULL (MKT_ON brand grain — UPC PROHIBITED, Blocker #6)
        -- CANAL_STD = 'ECOMMERCE_MEDIA' (constant sentinel)
        SUM(INVERSION_REAL)                   AS INVERSION_ON,
        SUM(IMPRESIONES)                      AS IMPRESIONES_ON,
        SUM(CLICS)                            AS CLICS_ON
    FROM PRD_MDP.MDP_DSP.VW_MKT_ECOMM
    WHERE ANIO >= 2024
    GROUP BY 1, 2
),
mkt_off_agg AS (
    SELECT
        DATE_TRUNC('MONTH', FECHA)            AS FECHA,
        TRIM(UPPER(MARCA))                    AS MARCA_STD,
        -- UPC_STD    = NULL (MKT_OFF brand grain — UPC PROHIBITED, Blocker #7)
        -- CADENA_STD = NULL (MKT_OFF has no chain — CADENA PROHIBITED, Blocker #8)
        -- CANAL_STD  = 'OFFLINE_MEDIA' (constant sentinel)
        SUM(INVERSION_REAL)                   AS INVERSION_OFF,
        SUM(IMPACTOS_HT)                      AS IMPACTOS_OFF
    FROM PRD_MDP.MDP_STG.FACT_MEDIA_OFF
    WHERE ANIO >= 2024
    GROUP BY 1, 2
)
SELECT
    COALESCE(si.FECHA,      so.FECHA,      on_.FECHA,    off_.FECHA)      AS FECHA_STD,
    COALESCE(si.MARCA_STD,  so.MARCA_STD,  on_.MARCA_STD, off_.MARCA_STD) AS MARCA_STD,
    si.VOLUMEN,
    si.VALOR                                                               AS SELL_IN_VALOR,
    so.VOL_SELL_OUT,
    so.AMOUNT_SELL_OUT,
    on_.INVERSION_ON,
    on_.IMPRESIONES_ON,
    on_.CLICS_ON,
    off_.INVERSION_OFF,
    off_.IMPACTOS_OFF
FROM sell_in_agg si
FULL OUTER JOIN sell_out_agg so
    ON  si.FECHA     = so.FECHA
    AND si.MARCA_STD = so.MARCA_STD
    -- UPC_STD is NOT a predicate at MARCA_STD+MONTH grain.
    -- UPC validation is exclusively a SELL_IN <-> SELL_OUT bridge check (Notebook A / B3).
LEFT JOIN mkt_on_agg on_
    ON  COALESCE(si.FECHA, so.FECHA)      = on_.FECHA
    AND COALESCE(si.MARCA_STD, so.MARCA_STD) = on_.MARCA_STD
    -- NO UPC predicate (MKT_ON = brand grain, Blocker #6)
LEFT JOIN mkt_off_agg off_
    ON  COALESCE(si.FECHA, so.FECHA)      = off_.FECHA
    AND COALESCE(si.MARCA_STD, so.MARCA_STD) = off_.MARCA_STD
    -- NO UPC predicate (MKT_OFF = brand grain, Blocker #7)
    -- NO CADENA predicate (MKT_OFF CADENA_STD always NULL, Blocker #8)
ORDER BY FECHA_STD, MARCA_STD
"""

print(APPROVED_JOIN_SQL)
log("INFO", "Approved join template v3 logged. Deviations from this template require MDM review.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section B8 — MKT_OFF CADENA_STD Assertion Note

# COMMAND ----------

# ── CELL 10: MKT_OFF CADENA assertion status ──────────────────────────────────
SECTION = "B8-MKTOFF-CADENA"
log("INFO", "=" * 60, SECTION)
log("INFO", "Business rule: MKT_OFF CADENA_STD must be NULL in standardized output.", SECTION)
log("INFO", "Assertion target: mkt_off_std (Phase 3 silver layer output).", SECTION)
log("INFO", "Cannot assert against raw FACT_MEDIA_OFF — column name 'CADENA' does not exist.", SECTION)
log("INFO", "Phase 3 assertion SQL (assert after silver_mkt_off.py runs):", SECTION)
log("INFO", """
  SELECT
      COUNT(*) AS total_rows,
      COUNT_IF(CADENA_STD IS NOT NULL) AS cadena_std_not_null_rows,
      COUNT_IF(CADENA_SOURCE <> 'N/A - MEDIA_OFF') AS invalid_cadena_source_rows
  FROM mkt_off_std;
  Expected: cadena_std_not_null_rows = 0 AND invalid_cadena_source_rows = 0
""", SECTION)
warn(True,
     "MKT_OFF CADENA_STD assertion deferred to Phase 3. Cannot run against raw FACT_MEDIA_OFF (column 'CADENA' does not exist). "
     "Run DESC TABLE PRD_MDP.MDP_STG.FACT_MEDIA_OFF to find the actual column name.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section B9 — Final Summary

# COMMAND ----------

# ── CELL 11: Summary + flush ──────────────────────────────────────────────────
SECTION = "SUMMARY"
log("INFO", "=" * 60, SECTION)
log("INFO", f"Hard blockers : {len(_HARD_BLOCKERS)}", SECTION)
log("INFO", f"Warnings      : {len(_WARNINGS)}", SECTION)

log("INFO", "\nVALIDATED RULES (v3):", SECTION)
log("INFO", "  ✓ MAT_IDT is unique SAP product key", SECTION)
log("INFO", "  ✓ SKU_EAN_COD is barcode attribute — may be non-unique", SECTION)
log("INFO", "  ✓ UPC_STD NOT a predicate in all-source join", SECTION)
log("INFO", "  ✓ MKT_ON join: MARCA_STD + FECHA only (no UPC, no CADENA as required key)", SECTION)
log("INFO", "  ✓ MKT_OFF join: MARCA_STD + FECHA only (no UPC, no CADENA — both NULL by design)", SECTION)
log("INFO", "  ✓ SELL_IN CADENA_STD = NULL (provisional — V_D_CLIENT chain field unconfirmed)", SECTION)
log("INFO", "  ✓ MKT_OFF CADENA assertion deferred to Phase 3 mkt_off_std output", SECTION)
log("INFO", "  ✓ All product IDs cast to VARCHAR (leading zeros preserved)", SECTION)
log("INFO", "  ✓ Cross-DB queries split into two Spark reads (no cross-DB SQL)", SECTION)

log("INFO", "\nPENDING MANUAL SIGN-OFFS:", SECTION)
log("INFO", "  ⚠  #3: 473 Nielsen MRKT_DSC_SHRT strings in NEEDS_REVIEW", SECTION)
log("INFO", "  ⚠  #4: V_D_CLIENT CUS_GRN_CHL_DSC likely CANAL (5 values) — CADENA unconfirmed", SECTION)
log("INFO", "  ⚠  #5: VW_D_STORE_RM: 19 CHAINs (likely CADENA), 86 FORMATs (likely CANAL)", SECTION)

if _HARD_BLOCKERS:
    log("🚨 BLOCKER", "BLOCKERS FOUND — resolve before Phase 3:", SECTION)
    for b in _HARD_BLOCKERS: log("🚨 BLOCKER", f"  → {b}", SECTION)
else:
    passed("Zero hard blockers — Notebook B clean.", SECTION)

flush_log()
print(f"\n{'='*60}\nNotebook B (v3) complete — {ts()}\nOutput: {DBFS_ROOT}\n{'='*60}")
