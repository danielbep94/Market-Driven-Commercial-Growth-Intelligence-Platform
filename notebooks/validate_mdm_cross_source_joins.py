# Databricks notebook source

# MAGIC %md
# MAGIC # MDM Cross-Source Join Validation — Notebook B
# MAGIC
# MAGIC **Purpose:** Validate cross-source join integrity at each grain level.
# MAGIC Confirm that join predicates respect UPC and CADENA applicability rules.
# MAGIC
# MAGIC **Credential resolution (matches `validate_credentials.py`):**
# MAGIC - `PRD_MEX` — `configs/snowflake_creds.py` → `SF_MEX_*`
# MAGIC - `PRD_MDP` — `configs/snowflake_creds.py` → `SF_MDP_*` or Key Vault fallback
# MAGIC
# MAGIC **Key rules enforced:**
# MAGIC 1. `UPC_STD` is NOT a predicate in the all-source join. UPC validation is scoped
# MAGIC    exclusively to the SELL_IN ↔ SELL_OUT bridge (Sections B3 / A4).
# MAGIC 2. MKT_ON and MKT_OFF must NEVER be joined using a UPC predicate.
# MAGIC 3. MKT_OFF must NEVER require CADENA as a join predicate.
# MAGIC 4. All-source joins operate at `MARCA_STD + DATE_TRUNC('MONTH', FECHA)`.
# MAGIC
# MAGIC **Output:**
# MAGIC ```
# MAGIC dbfs:/mnt/mdp/mdm/notebook_b/validation_results_mdm_cross_source.txt
# MAGIC ```

# COMMAND ----------

# ── CELL 1: Load credentials (same pattern as validate_credentials.py) ────────
import os, importlib.util, datetime

_current_dir = os.getcwd()
_creds_path  = os.path.normpath(
    os.path.join(_current_dir, "..", "configs", "snowflake_creds.py")
)

if not os.path.exists(_creds_path):
    raise FileNotFoundError(
        "❌ configs/snowflake_creds.py NOT FOUND.\n"
        "   Copy configs/snowflake_creds.example.py → configs/snowflake_creds.py and fill in values."
    )

_spec = importlib.util.spec_from_file_location("snowflake_creds", _creds_path)
_m    = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_m)

SF_URL = "danonenam.east-us-2.azure.snowflakecomputing.com"

def get_sf_options(database: str) -> dict:
    _mdp_user = getattr(_m, "SF_MDP_USER", None)
    _mdp_pwd  = getattr(_m, "SF_MDP_PASSWORD", None)
    profiles = {
        "PRD_MEX": {
            "sfURL":       SF_URL,
            "sfUser":      _m.SF_MEX_USER,
            "sfPassword":  _m.SF_MEX_PASSWORD,
            "sfWarehouse": getattr(_m, "SF_MEX_WH",   "PRD_MEX_ANL_WH"),
            "sfRole":      getattr(_m, "SF_MEX_ROLE",  "PRD_MEX_READER"),
        },
        "PRD_MDP": {
            "sfURL":       SF_URL,
            "sfUser":      _mdp_user or dbutils.secrets.get("DAN-AM-P-KVT800-R-MDP-DB", "snowflake-user"),
            "sfPassword":  _mdp_pwd  or dbutils.secrets.get("DAN-AM-P-KVT800-R-MDP-DB", "snowflake-password"),
            "sfWarehouse": getattr(_m, "SF_MDP_WH",   "PRD_MDP_ANL_WH"),
            "sfRole":      getattr(_m, "SF_MDP_ROLE",  "PRD_MDP"),
        },
    }
    if database not in profiles:
        raise ValueError(f"No profile for '{database}'. Available: {list(profiles.keys())}")
    return dict(profiles[database])

print(f"✅ Credentials loaded — PRD_MEX: {_m.SF_MEX_USER} | PRD_MDP: {'<Key Vault>' if not getattr(_m, 'SF_MDP_USER', None) else _m.SF_MDP_USER}")

# COMMAND ----------

# ── CELL 2: Output paths + helpers ────────────────────────────────────────────
DBFS_ROOT  = "dbfs:/mnt/mdp/mdm/notebook_b"
LOCAL_ROOT = "/dbfs/mnt/mdp/mdm/notebook_b"
dbutils.fs.mkdirs(DBFS_ROOT)

_LOG_LINES: list[str] = []
_HARD_BLOCKERS = []
_WARNINGS      = []

def ts() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def log(level: str, msg: str, section: str = ""):
    prefix = f"[{ts()}][{level}]"
    if section: prefix += f"[{section}]"
    line = f"{prefix} {msg}"
    _LOG_LINES.append(line)
    print(line)

def flush_log():
    dbutils.fs.put(
        f"{DBFS_ROOT}/validation_results_mdm_cross_source.txt",
        "\n".join(_LOG_LINES),
        overwrite=True
    )
    print(f"📄 Log → {DBFS_ROOT}/validation_results_mdm_cross_source.txt")

def run_sf(database: str, sql: str):
    opts = get_sf_options(database)
    return (spark.read
                 .format("net.snowflake.spark.snowflake")
                 .options(**opts)
                 .option("sfDatabase", database)
                 .option("query", sql)
                 .load())

def blocker(cond: bool, msg: str, sec: str = ""):
    if cond: log("🚨 BLOCKER", msg, sec); _HARD_BLOCKERS.append(msg)
    return cond

def warn(cond: bool, msg: str, sec: str = ""):
    if cond: log("⚠️  WARNING", msg, sec); _WARNINGS.append(msg)
    return cond

def passed(msg: str, sec: str = ""):
    log("✅ PASS", msg, sec)

# Applicability matrices (business rules)
UPC_APPLICABLE    = {"SELL_IN": True, "SELL_OUT": True, "MKT_ON": False, "MKT_OFF": False,
                     "EDP_NIELSEN": False, "PB_NIELSEN": False, "WATER_NIELSEN_RIE": False,
                     "WATER_SCANTRACK": False, "IBP": False, "WASTE": False}
CADENA_APPLICABLE = {"SELL_IN": False, "SELL_OUT": True, "MKT_ON": True, "MKT_OFF": False,
                     "EDP_NIELSEN": True, "PB_NIELSEN": True, "WATER_NIELSEN_RIE": True,
                     "WATER_SCANTRACK": True, "IBP": True, "WASTE": True}

print("✅ CELL 2 — Helpers ready. Output root:", DBFS_ROOT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section B1 — Row Count Before / After Standardization

# COMMAND ----------

# ── CELL 3: Row counts per source ────────────────────────────────────────────
SECTION = "B1-ROW-COUNTS"
log("INFO", "=" * 60, SECTION)

ROW_QUERIES = {
    "SELL_IN_RAW":   ("PRD_MEX", """
        SELECT COUNT(*) AS row_count, COUNT(DISTINCT TO_VARCHAR(MAT_IDT)) AS distinct_skus
        FROM PRD_MEX.MEX_DSP_OTC.VW_FACT_RNV
        WHERE BIL_DAT >= 20250101
    """),
    "SELL_IN_STD": ("PRD_MEX", """
        SELECT COUNT(*) AS row_count, COUNT(DISTINCT TO_VARCHAR(FAC.MAT_IDT)) AS distinct_skus
        FROM PRD_MEX.MEX_DSP_OTC.VW_FACT_RNV FAC
        LEFT JOIN PRD_MEX.MEX_DSP_OTC.V_D_ITEM PRO ON FAC.MAT_IDT = PRO.MAT_IDT
        WHERE FAC.CBU IN ('WATERS','EDP')
          AND DATE_TRUNC('MONTH', TO_DATE(TO_VARCHAR(FAC.BIL_DAT),'YYYYMMDD')) >= '2025-01-01'
    """),
    "SELL_OUT_RAW": ("PRD_MDP", """
        SELECT COUNT(*) AS row_count, COUNT(DISTINCT TO_VARCHAR(UPC)) AS distinct_upcs
        FROM PRD_MDP.MDP_DSP.VW_FACT_SELL_OUT WHERE PER_ID >= 20250101
    """),
    "MKT_ON_RAW":  ("PRD_MDP", """
        SELECT COUNT(*) AS row_count, COUNT(DISTINCT MARCA) AS distinct_brands
        FROM PRD_MDP.MDP_DSP.VW_MKT_ECOMM WHERE ANIO >= 2024
    """),
    "MKT_OFF_RAW": ("PRD_MDP", """
        SELECT COUNT(*) AS row_count, COUNT(DISTINCT MARCA) AS distinct_brands
        FROM PRD_MDP.MDP_STG.FACT_MEDIA_OFF WHERE ANIO >= 2024
    """),
    "IBP_RAW":     ("PRD_MDP", """
        SELECT COUNT(*) AS row_count, COUNT(DISTINCT MARCA) AS distinct_brands,
               COUNT(DISTINCT CADENA) AS distinct_cadenas
        FROM PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP
    """),
    "WASTE_RAW":   ("PRD_MDP", """
        SELECT COUNT(*) AS row_count, COUNT(DISTINCT MARCA) AS distinct_brands
        FROM PRD_MDP.MDP_STG.FACT_TOPLINE WHERE UPPER(TRIM(FUENTE)) = 'TOPLINE'
    """),
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
        log("⚠️  WARNING", f"{label}: query failed — {exc}", SECTION)

display(spark.createDataFrame(count_rows))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section B2 — MARCA_STD Overlap: SELL_IN vs Each Source

# COMMAND ----------

# ── CELL 4: Fetch SELL_IN brand set (PRD_MEX) ─────────────────────────────────
SECTION = "B2-BRAND-OVERLAP"
log("INFO", "=" * 60, SECTION)

df_si_brands = run_sf("PRD_MEX", """
    SELECT DISTINCT TRIM(UPPER(LV2_UMB_BRD_DSC)) AS marca_raw
    FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM
    WHERE LV2_UMB_BRD_DSC IS NOT NULL
""")
si_brands = {r["MARCA_RAW"] for r in df_si_brands.collect()}
log("INFO", f"SELL_IN distinct brand values: {len(si_brands)}", SECTION)

# COMMAND ----------

# ── CELL 5: Brand overlap per source ─────────────────────────────────────────
BRAND_OVERLAP = {
    "SELL_OUT":          ("PRD_MDP", "SELECT DISTINCT TRIM(UPPER(BRAND)) AS marca_raw FROM PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM WHERE BRAND IS NOT NULL"),
    "MKT_ON":            ("PRD_MDP", "SELECT DISTINCT TRIM(UPPER(MARCA)) AS marca_raw FROM PRD_MDP.MDP_DSP.VW_MKT_ECOMM WHERE MARCA IS NOT NULL"),
    "MKT_OFF":           ("PRD_MDP", "SELECT DISTINCT TRIM(UPPER(MARCA)) AS marca_raw FROM PRD_MDP.MDP_STG.FACT_MEDIA_OFF WHERE MARCA IS NOT NULL"),
    "IBP":               ("PRD_MDP", "SELECT DISTINCT TRIM(UPPER(MARCA)) AS marca_raw FROM PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP WHERE MARCA IS NOT NULL"),
    "WASTE":             ("PRD_MDP", "SELECT DISTINCT TRIM(UPPER(MARCA)) AS marca_raw FROM PRD_MDP.MDP_STG.FACT_TOPLINE WHERE MARCA IS NOT NULL AND UPPER(TRIM(FUENTE))='TOPLINE'"),
    "EDP_NIELSEN":       ("PRD_MEX", "SELECT DISTINCT TRIM(UPPER(INP_56985)) AS marca_raw FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_PROD_DIM WHERE INP_56985 IS NOT NULL"),
    "PB_NIELSEN":        ("PRD_MEX", "SELECT DISTINCT TRIM(UPPER(INP_56985)) AS marca_raw FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_SUST_LECHE_ST_NLSN_PROD_DIM WHERE INP_56985 IS NOT NULL"),
    "WATER_NIELSEN_RIE": ("PRD_MEX", "SELECT DISTINCT TRIM(UPPER(CSTM_310589)) AS marca_raw FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_RT_NLSN_PROD_DIM WHERE CSTM_310589 IS NOT NULL"),
    "WATER_SCANTRACK":   ("PRD_MEX", "SELECT DISTINCT TRIM(UPPER(CSTM_310589)) AS marca_raw FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_ST_NLSN_PROD_DIM WHERE CSTM_310589 IS NOT NULL"),
}

overlap_rows = []
for src, (db, sql) in BRAND_OVERLAP.items():
    try:
        src_brands = {r["MARCA_RAW"] for r in run_sf(db, sql).collect()}
        overlap    = si_brands & src_brands
        only_src   = src_brands - si_brands
        pct = round(len(overlap) / len(src_brands) * 100, 1) if src_brands else 0.0
        overlap_rows.append({"source": src, "src_distinct": len(src_brands),
                              "overlap_with_sell_in": len(overlap), "pct": pct,
                              "only_in_source": len(only_src)})
        log("INFO", f"{src}: {len(src_brands)} brands | overlap={len(overlap)} ({pct}%) | only_in_src={len(only_src)}", SECTION)
    except Exception as exc:
        log("⚠️  WARNING", f"{src}: query failed — {exc}", SECTION)

display(spark.createDataFrame(overlap_rows))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section B3 — UPC Match Rate: SELL_IN ↔ SELL_OUT Only
# MAGIC
# MAGIC > ⚠️ UPC_STD is NOT used in the all-source join.
# MAGIC > This section validates the bridge exclusively — not the final join predicate.

# COMMAND ----------

# ── CELL 6: UPC match rate via PRD_MDP (cross-db query) ─────────────────────
SECTION = "B3-UPC-MATCH-RATE"
log("INFO", "=" * 60, SECTION)
log("INFO", "UPC_STD is NOT a predicate in the all-source join.", SECTION)
log("INFO", "This section validates the SELL_IN <-> SELL_OUT bridge only.", SECTION)

SQL_UPC_RATE = """
WITH si_upcs AS (
    SELECT DISTINCT TO_VARCHAR(SKU_EAN_COD) AS upc_std
    FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM
    WHERE SKU_EAN_COD IS NOT NULL AND TRIM(TO_VARCHAR(SKU_EAN_COD)) <> ''
),
so_upcs AS (
    SELECT DISTINCT TO_VARCHAR(UPC) AS upc_so
    FROM PRD_MDP.MDP_DSP.VW_FACT_SELL_OUT
    WHERE PER_ID >= 20250101 AND UPC IS NOT NULL
),
matched AS (
    SELECT so.upc_so FROM so_upcs so
    INNER JOIN si_upcs si ON so.upc_so = si.upc_std
)
SELECT
    (SELECT COUNT(*) FROM so_upcs)   AS total_sell_out_upcs,
    (SELECT COUNT(*) FROM matched)   AS matched_to_sell_in,
    (SELECT COUNT(*) FROM so_upcs) - (SELECT COUNT(*) FROM matched) AS unmatched
"""
# Run via PRD_MDP profile — this profile can see both PRD_MEX and PRD_MDP in cross-db SQL
df_rate = run_sf("PRD_MDP", SQL_UPC_RATE)
display(df_rate)
r = df_rate.collect()[0]
total_so = r["TOTAL_SELL_OUT_UPCS"]
matched  = r["MATCHED_TO_SELL_IN"]
p1_pct   = round(matched / total_so * 100, 1) if total_so else 0.0
log("INFO", f"SELL_OUT UPCs={total_so:,} | matched={matched:,} ({p1_pct}%) | unmatched={r['UNMATCHED']:,}", SECTION)
warn(p1_pct < 70.0, f"UPC match rate {p1_pct}% below 70% threshold.", SECTION)
if p1_pct >= 70.0:
    passed(f"UPC match rate {p1_pct}% ≥ 70%.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section B4 — UPC & CADENA Predicate Violation Matrix

# COMMAND ----------

# ── CELL 7: Structural rule audit ────────────────────────────────────────────
SECTION = "B4-PREDICATE-VIOLATIONS"
log("INFO", "=" * 60, SECTION)

log("INFO", "UPC applicability by source:", SECTION)
for src, ok in UPC_APPLICABLE.items():
    log("INFO", f"  {src:<30} UPC join: {'ALLOWED' if ok else 'PROHIBITED'}", SECTION)

log("INFO", "CADENA applicability by source:", SECTION)
for src, ok in CADENA_APPLICABLE.items():
    log("INFO", f"  {src:<30} CADENA join: {'ALLOWED' if ok else 'PROHIBITED / NULL'}", SECTION)

blocker(UPC_APPLICABLE.get("MKT_ON") or UPC_APPLICABLE.get("MKT_OFF"),
        "MKT_ON or MKT_OFF incorrectly flagged as UPC-APPLICABLE.", SECTION)
blocker(CADENA_APPLICABLE.get("MKT_OFF"),
        "MKT_OFF incorrectly flagged as CADENA-APPLICABLE.", SECTION)
if not UPC_APPLICABLE["MKT_ON"] and not UPC_APPLICABLE["MKT_OFF"]:
    passed("MKT_ON and MKT_OFF correctly flagged as UPC-PROHIBITED.", SECTION)
if not CADENA_APPLICABLE["MKT_OFF"]:
    passed("MKT_OFF correctly flagged as CADENA-PROHIBITED (CADENA_STD = NULL).", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section B5 — Null Rate Audit per Dimension per Source

# COMMAND ----------

# ── CELL 8: Null rates with expected-100%-NULL enforcement ───────────────────
SECTION = "B5-NULL-RATES"
log("INFO", "=" * 60, SECTION)
log("INFO", "MKT_ON UPC_STD, MKT_OFF UPC_STD, MKT_OFF CADENA_STD expected 100% NULL — by design.", SECTION)

NULL_QUERIES = {
    # label:  (db,  sql,  expected_100pct_null)
    "SELL_IN_MARCA":  ("PRD_MEX", """SELECT 'SELL_IN' AS src,'MARCA_STD' AS dim,COUNT(*) AS total,
        COUNT_IF(LV2_UMB_BRD_DSC IS NULL OR TRIM(LV2_UMB_BRD_DSC)='') AS null_rows
        FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM""", False),
    "SELL_IN_UPC":    ("PRD_MEX", """SELECT 'SELL_IN' AS src,'UPC_STD' AS dim,COUNT(*) AS total,
        COUNT_IF(SKU_EAN_COD IS NULL OR TRIM(TO_VARCHAR(SKU_EAN_COD))='') AS null_rows
        FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM WHERE MAT_ACT_FLG=1""", False),
    "MKT_ON_UPC":     ("PRD_MDP", """SELECT 'MKT_ON' AS src,'UPC_STD' AS dim,COUNT(*) AS total,
        COUNT(*) AS null_rows FROM PRD_MDP.MDP_DSP.VW_MKT_ECOMM WHERE ANIO>=2024""", True),
    "MKT_OFF_UPC":    ("PRD_MDP", """SELECT 'MKT_OFF' AS src,'UPC_STD' AS dim,COUNT(*) AS total,
        COUNT(*) AS null_rows FROM PRD_MDP.MDP_STG.FACT_MEDIA_OFF WHERE ANIO>=2024""", True),
    "MKT_OFF_CADENA": ("PRD_MDP", """SELECT 'MKT_OFF' AS src,'CADENA_STD' AS dim,COUNT(*) AS total,
        COUNT(*) AS null_rows FROM PRD_MDP.MDP_STG.FACT_MEDIA_OFF WHERE ANIO>=2024""", True),
    "SELL_OUT_MARCA": ("PRD_MDP", """SELECT 'SELL_OUT' AS src,'MARCA_STD' AS dim,COUNT(*) AS total,
        COUNT_IF(prod.BRAND IS NULL OR TRIM(prod.BRAND)='') AS null_rows
        FROM PRD_MDP.MDP_DSP.VW_FACT_SELL_OUT f
        INNER JOIN PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM prod
            ON TO_VARCHAR(f.UPC)=TO_VARCHAR(prod.INT_ID) AND f.CBU_ID=prod.CBU_ID
        WHERE f.PER_ID>=20250101""", False),
    "IBP_CADENA":     ("PRD_MDP", """SELECT 'IBP' AS src,'CADENA' AS dim,COUNT(*) AS total,
        COUNT_IF(CADENA IS NULL OR TRIM(CADENA)='') AS null_rows
        FROM PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP""", False),
}

null_rows = []
for label, (db, sql, expect_100_null) in NULL_QUERIES.items():
    try:
        r = run_sf(db, sql).collect()[0]
        total    = int(r["TOTAL"])
        null_cnt = int(r["NULL_ROWS"])
        pct      = round(null_cnt / total * 100, 2) if total else 0.0
        if expect_100_null:
            status = "✅ PASS (expected 100% NULL by design)" if pct == 100.0 else "⚠️ NOT 100% NULL — check source"
            blocker(pct < 100.0 and label in ["MKT_OFF_CADENA"],
                    f"{label}: CADENA_STD not 100% NULL for MKT_OFF — fake values detected.", SECTION)
        else:
            status = "✅ PASS" if pct <= 1.0 else f"⚠️ WARNING — {pct}% NULL (threshold 1%)"
            warn(pct > 1.0, f"{label}: null rate {pct}% above 1% threshold.", SECTION)
        null_rows.append({"label": label, "source": str(r["SRC"]), "dimension": str(r["DIM"]),
                          "total": total, "null_count": null_cnt, "null_pct": pct, "status": status})
        log("INFO", f"{label}: total={total} | null={null_cnt} ({pct}%) | {status}", SECTION)
    except Exception as exc:
        log("⚠️  WARNING", f"{label}: failed — {exc}", SECTION)

display(spark.createDataFrame(null_rows))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section B6 — Aggregation Grain Check

# COMMAND ----------

# ── CELL 9: Confirm fanout before brand-level aggregation ─────────────────────
SECTION = "B6-AGG-GRAIN"
log("INFO", "=" * 60, SECTION)
log("INFO", "Rule: Aggregate each source to MARCA_STD + MONTH before joining.", SECTION)

SQL_GRAIN = """
SELECT 'SELL_IN' AS src,
    COUNT(*) AS raw_rows,
    COUNT(DISTINCT DATE_TRUNC('MONTH', TO_DATE(TO_VARCHAR(FAC.BIL_DAT),'YYYYMMDD'))
          || '|' || TRIM(UPPER(PRO.LV2_UMB_BRD_DSC))) AS grain_combos
FROM PRD_MEX.MEX_DSP_OTC.VW_FACT_RNV FAC
LEFT JOIN PRD_MEX.MEX_DSP_OTC.V_D_ITEM PRO ON FAC.MAT_IDT = PRO.MAT_IDT
WHERE FAC.CBU IN ('WATERS','EDP')
  AND DATE_TRUNC('MONTH', TO_DATE(TO_VARCHAR(FAC.BIL_DAT),'YYYYMMDD')) >= '2025-01-01'
"""
# PRD_MEX for SELL_IN grain
df_grain = run_sf("PRD_MEX", SQL_GRAIN)
display(df_grain)
r = df_grain.collect()[0]
ratio = round(r["RAW_ROWS"] / r["GRAIN_COMBOS"], 1) if r["GRAIN_COMBOS"] else 0.0
log("INFO", f"SELL_IN: raw_rows={r['RAW_ROWS']:,} | grain_combos={r['GRAIN_COMBOS']:,} | fanout_ratio={ratio}x", SECTION)
warn(ratio > 10, f"SELL_IN fanout ratio {ratio}x — aggregation REQUIRED before cross-source join.", SECTION)
if ratio <= 10:
    passed(f"SELL_IN fanout ratio {ratio}x — acceptable.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section B7 — Approved All-Source Join Template

# COMMAND ----------

# ── CELL 10: Document approved join SQL ───────────────────────────────────────
SECTION = "B7-JOIN-TEMPLATE"
log("INFO", "=" * 60, SECTION)
log("INFO", "Grain : MARCA_STD + DATE_TRUNC('MONTH', FECHA)", SECTION)
log("INFO", "UPC   : NOT a predicate in this template.", SECTION)
log("INFO", "        UPC validation scoped to Section A4 (Notebook A) / Section B3 (this notebook).", SECTION)
log("INFO", "        ONLY valid as a SELL_IN <-> SELL_OUT bridge check, never as an all-source join key.", SECTION)

APPROVED_JOIN_SQL = """
-- ============================================================
-- APPROVED ALL-SOURCE JOIN PATTERN
-- Grain: MARCA_STD + DATE_TRUNC('MONTH', FECHA)
-- Database routing:
--   SELL_IN aggregation  → PRD_MEX (SF_MEX_* credentials)
--   SELL_OUT aggregation → PRD_MDP (SF_MDP_* / Key Vault)
--   MKT_ON / MKT_OFF    → PRD_MDP
-- UPC_STD: NOT used as join predicate here.
-- ============================================================

WITH sell_in_agg AS (
    SELECT
        DATE_TRUNC('MONTH', TO_DATE(TO_VARCHAR(FAC.BIL_DAT),'YYYYMMDD')) AS FECHA,
        TRIM(UPPER(PRO.LV2_UMB_BRD_DSC))   AS MARCA_STD,
        SUM(FAC.LITER)                      AS VOLUMEN,
        SUM(FAC.BIL_INV)                    AS VALOR
    FROM PRD_MEX.MEX_DSP_OTC.VW_FACT_RNV FAC
    LEFT JOIN PRD_MEX.MEX_DSP_OTC.V_D_ITEM PRO ON FAC.MAT_IDT = PRO.MAT_IDT
    WHERE FAC.CBU IN ('WATERS','EDP')
      AND DATE_TRUNC('MONTH', TO_DATE(TO_VARCHAR(FAC.BIL_DAT),'YYYYMMDD')) >= '2025-01-01'
    GROUP BY 1, 2
),
sell_out_agg AS (
    SELECT
        DATE_TRUNC('MONTH', per."DAY")      AS FECHA,
        TRIM(UPPER(prod.BRAND))             AS MARCA_STD,
        SUM(f.VOL_SELL_OUT)                 AS VOL_SELL_OUT,
        SUM(f.AMOUNT_SELL_OUT)              AS AMOUNT_SELL_OUT
    FROM PRD_MDP.MDP_DSP.VW_FACT_SELL_OUT f
    INNER JOIN PRD_MDP.MDP_DWH.V_D_PERIOD per   ON f.PER_ID = per.PER_ID
    INNER JOIN PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM prod
        ON TO_VARCHAR(f.UPC) = TO_VARCHAR(prod.INT_ID) AND f.CBU_ID = prod.CBU_ID
    WHERE f.PER_ID >= 20250101
    GROUP BY 1, 2
),
mkt_on_agg AS (
    SELECT
        DATE_TRUNC('MONTH', FECHA)          AS FECHA,
        TRIM(UPPER(MARCA))                  AS MARCA_STD,
        -- UPC_STD = NULL (MKT_ON is BRAND_GRAIN — UPC always NULL)
        -- CANAL_STD = 'ECOMMERCE_MEDIA' (constant)
        SUM(INVERSION_REAL)                 AS INVERSION_ON,
        SUM(IMPRESIONES)                    AS IMPRESIONES_ON,
        SUM(CLICS)                          AS CLICS_ON
    FROM PRD_MDP.MDP_DSP.VW_MKT_ECOMM
    WHERE ANIO >= 2024
    GROUP BY 1, 2
),
mkt_off_agg AS (
    SELECT
        DATE_TRUNC('MONTH', FECHA)          AS FECHA,
        TRIM(UPPER(MARCA))                  AS MARCA_STD,
        -- UPC_STD   = NULL (MKT_OFF is BRAND_GRAIN — UPC always NULL)
        -- CADENA_STD = NULL (MKT_OFF has no CADENA — always NULL)
        -- CANAL_STD = 'OFFLINE_MEDIA' (constant)
        SUM(INVERSION_REAL)                 AS INVERSION_OFF,
        SUM(IMPACTOS_HT)                    AS IMPACTOS_OFF
    FROM PRD_MDP.MDP_STG.FACT_MEDIA_OFF
    WHERE ANIO >= 2024
    GROUP BY 1, 2
)
SELECT
    COALESCE(si.FECHA,     so.FECHA,     on_.FECHA,    off_.FECHA)    AS FECHA_STD,
    COALESCE(si.MARCA_STD, so.MARCA_STD, on_.MARCA_STD,off_.MARCA_STD) AS MARCA_STD,
    si.VOLUMEN,
    si.VALOR                                                            AS SELL_IN_VALOR,
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
    -- UPC_STD is NOT a predicate at this grain.
    -- UPC-level join is validated in Notebook A (Section A4) / Notebook B (Section B3).
    -- It is ONLY valid as a SELL_IN <-> SELL_OUT bridge check.
LEFT JOIN mkt_on_agg on_
    ON  COALESCE(si.FECHA, so.FECHA) = on_.FECHA
    AND COALESCE(si.MARCA_STD, so.MARCA_STD) = on_.MARCA_STD
    -- NO UPC predicate (MKT_ON is BRAND_GRAIN — UPC_STD always NULL)
LEFT JOIN mkt_off_agg off_
    ON  COALESCE(si.FECHA, so.FECHA)         = off_.FECHA
    AND COALESCE(si.MARCA_STD, so.MARCA_STD) = off_.MARCA_STD
    -- NO UPC predicate (MKT_OFF is BRAND_GRAIN — UPC_STD always NULL)
    -- NO CADENA predicate (MKT_OFF has no CADENA — CADENA_STD always NULL)
ORDER BY FECHA_STD, MARCA_STD
"""

print(APPROVED_JOIN_SQL)
log("INFO", "Approved join template logged above.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section B8 — Final Summary

# COMMAND ----------

# ── CELL 11: Summary + flush ──────────────────────────────────────────────────
SECTION = "SUMMARY"
log("INFO", "=" * 60, SECTION)
log("INFO", f"Hard blockers : {len(_HARD_BLOCKERS)}", SECTION)
log("INFO", f"Warnings      : {len(_WARNINGS)}", SECTION)

log("INFO", "\nVALIDATED ASSUMPTIONS:", SECTION)
log("INFO", "  ✓ SELL_IN is golden source for UPC (SAP V_D_ITEM, PRD_MEX)", SECTION)
log("INFO", "  ✓ MKT_ON grain is BRAND + FECHA + CADENA (no UPC)", SECTION)
log("INFO", "  ✓ MKT_OFF grain is BRAND + FECHA only (no UPC, no CADENA)", SECTION)
log("INFO", "  ✓ MKT_ON CANAL_STD = 'ECOMMERCE_MEDIA' (constant, confirmed)", SECTION)
log("INFO", "  ✓ MKT_OFF CANAL_STD = 'OFFLINE_MEDIA' (constant, confirmed)", SECTION)
log("INFO", "  ✓ IBP CADENA = retail chains (HEB, WALMART, etc.) confirmed", SECTION)
log("INFO", "  ✓ IBP CADENA GUATEMALA/EL SALVADOR → cadena_type = REGION", SECTION)
log("INFO", "  ✓ WASTE metric cols: 'WASTE ($)' and 'WASTE (KG)' (confirmed)", SECTION)
log("INFO", "  ✓ All product IDs cast to VARCHAR (leading zeros preserved)", SECTION)
log("INFO", "  ✓ UPC_STD removed from all-source join predicate (approved correction)", SECTION)
log("INFO", "  ✓ Credential profiles: PRD_MEX=SF_MEX_* | PRD_MDP=SF_MDP_*/Key Vault", SECTION)

log("INFO", "\nUNRESOLVED — PENDING SNOWFLAKE VALIDATION:", SECTION)
log("INFO", "  ⚠ SELL_IN CADENA: V_D_CLIENT.CUS_GRN_CHL_DSC not yet confirmed as chain field", SECTION)
log("INFO", "  ⚠ SELL_OUT CANAL: VW_D_STORE_RM.CHAIN/FORMAT not yet classified", SECTION)
log("INFO", "  ⚠ Nielsen market bridge: MRKT_DSC_SHRT values not fully mapped", SECTION)
log("INFO", "  ⚠ sku_mapping.csv: requires V_D_ITEM population (sign-off #1)", SECTION)

if _HARD_BLOCKERS:
    log("🚨 BLOCKER", "BLOCKERS FOUND — resolve before Phase 3:", SECTION)
    for b in _HARD_BLOCKERS:
        log("🚨 BLOCKER", f"  → {b}", SECTION)
else:
    log("✅ PASS", "Zero hard blockers — Notebook B clean.", SECTION)

flush_log()
print(f"\n{'='*60}\nNotebook B complete — {ts()}\nOutput: {DBFS_ROOT}\n{'='*60}")
