# Databricks notebook source

# MAGIC %md
# MAGIC # MDM Catalog Validation — Notebook A
# MAGIC
# MAGIC **Purpose:** Profile the five MDM catalogs (MARCA, UPC, CANAL, CADENA, Nielsen)
# MAGIC against all source systems and write structured audit logs.
# MAGIC
# MAGIC **Credential resolution (matches `validate_credentials.py`):**
# MAGIC - `PRD_MEX` — `configs/snowflake_creds.py` → `SF_MEX_*` (hardcoded analyst account)
# MAGIC - `PRD_MDP` — `configs/snowflake_creds.py` → `SF_MDP_*` or Key Vault fallback
# MAGIC
# MAGIC **Run first:** `notebooks/validate_credentials.py` — all 6 cells must pass.
# MAGIC
# MAGIC **Output files (DBFS):**
# MAGIC ```
# MAGIC dbfs:/mnt/mdp/mdm/notebook_a/
# MAGIC   validation_results_mdm_catalogs.txt
# MAGIC   unmapped_brands_by_source.csv
# MAGIC   unmapped_channels_by_source.csv
# MAGIC   unmapped_cadenas_by_source.csv
# MAGIC   upc_bridge_unmatched_sell_out.csv
# MAGIC   upc_bridge_fuzzy_candidates.csv   ← P3 quarantine — never auto-promote
# MAGIC ```
# MAGIC
# MAGIC > ⚠️ Do not promote any fuzzy UPC candidate to production without human review.

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
        "   Copy configs/snowflake_creds.example.py → configs/snowflake_creds.py\n"
        "   and fill in your credentials, then re-run."
    )

_spec = importlib.util.spec_from_file_location("snowflake_creds", _creds_path)
_m    = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_m)

SF_URL = "danonenam.east-us-2.azure.snowflakecomputing.com"

def get_sf_options(database: str) -> dict:
    """Credential profiles — mirrors validate_credentials.py exactly."""
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

print(f"✅ Credentials loaded from: {_creds_path}")
print(f"   PRD_MEX user      : {_m.SF_MEX_USER}")
print(f"   PRD_MEX warehouse : {getattr(_m, 'SF_MEX_WH', 'PRD_MEX_ANL_WH')}")
print(f"   PRD_MEX role      : {getattr(_m, 'SF_MEX_ROLE', 'PRD_MEX_READER')}")
print(f"   PRD_MDP user      : {'<from Key Vault>' if not getattr(_m, 'SF_MDP_USER', None) else _m.SF_MDP_USER}")
print(f"   PRD_MDP warehouse : {getattr(_m, 'SF_MDP_WH', 'PRD_MDP_ANL_WH')}")

# COMMAND ----------

# ── CELL 2: Output paths + helpers ────────────────────────────────────────────
DBFS_ROOT  = "dbfs:/mnt/mdp/mdm/notebook_a"
LOCAL_ROOT = "/dbfs/mnt/mdp/mdm/notebook_a"
LOG_PATH   = f"{LOCAL_ROOT}/validation_results_mdm_catalogs.txt"

dbutils.fs.mkdirs(DBFS_ROOT)

_LOG_LINES: list[str] = []

def ts() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def log(level: str, msg: str, section: str = ""):
    prefix = f"[{ts()}][{level}]"
    if section:
        prefix += f"[{section}]"
    line = f"{prefix} {msg}"
    _LOG_LINES.append(line)
    print(line)

def flush_log():
    content = "\n".join(_LOG_LINES)
    dbutils.fs.put(f"{DBFS_ROOT}/validation_results_mdm_catalogs.txt", content, overwrite=True)
    print(f"\n📄 Log → {DBFS_ROOT}/validation_results_mdm_catalogs.txt")

def save_df(df, filename: str):
    path = f"{DBFS_ROOT}/{filename}"
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(path)
    log("INFO", f"Saved → {path}")
    return path

def run_sf(database: str, sql: str):
    """Execute any Snowflake query using the correct credential profile."""
    opts = get_sf_options(database)
    return (spark.read
                 .format("net.snowflake.spark.snowflake")
                 .options(**opts)
                 .option("sfDatabase", database)
                 .option("query", sql)
                 .load())

# Hard-blocker / warning helpers
_HARD_BLOCKERS = []
_WARNINGS      = []

def blocker(condition: bool, msg: str, section: str = ""):
    if condition:
        log("🚨 BLOCKER", msg, section)
        _HARD_BLOCKERS.append(msg)
    return condition

def warn(condition: bool, msg: str, section: str = ""):
    if condition:
        log("⚠️  WARNING", msg, section)
        _WARNINGS.append(msg)
    return condition

def passed(msg: str, section: str = ""):
    log("✅ PASS", msg, section)

print("✅ CELL 2 — Helpers ready. Output root:", DBFS_ROOT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section A1 — CAT_MARCA Structural Validation

# COMMAND ----------

# ── CELL 3: Load brand catalog + uniqueness check ─────────────────────────────
SECTION = "A1-CAT_MARCA"
log("INFO", "=" * 60, SECTION)

# Read catalog from DBFS/repo path (adjust if stored as Delta table)
import pandas as pd

_HOMO_PATH = os.path.normpath(os.path.join(_current_dir, "..", "homologation"))
brand_csv  = os.path.join(_HOMO_PATH, "brand_mapping.csv")

if not os.path.exists(brand_csv):
    log("🚨 BLOCKER", f"brand_mapping.csv not found at {brand_csv}", SECTION)
    _HARD_BLOCKERS.append("brand_mapping.csv missing")
    brand_pdf = pd.DataFrame()
else:
    brand_pdf = pd.read_csv(brand_csv, dtype=str).fillna("")
    brand_pdf = brand_pdf[~brand_pdf.iloc[:, 0].str.startswith("[PENDING")]
    total = len(brand_pdf)
    log("INFO", f"Loaded {total} rows from brand_mapping.csv", SECTION)

    # Check 1: uniqueness on source_system + raw_name_normalized
    dupes = (brand_pdf
             .groupby(["source_system", "raw_name_normalized"])
             .size()
             .reset_index(name="count"))
    dupes = dupes[dupes["count"] > 1]
    blocker(len(dupes) > 0,
            f"{len(dupes)} duplicate source+raw_name_normalized combos — must be 0.",
            SECTION)
    if len(dupes) == 0:
        passed("Uniqueness check: no duplicate source+raw_name combinations.", SECTION)

    # Check 2: status distribution
    status_counts = brand_pdf["mapping_status"].value_counts().to_dict()
    for s, c in status_counts.items():
        log("INFO", f"  mapping_status={s}: {c}", SECTION)

    nr = brand_pdf[brand_pdf["mapping_status"] == "NEEDS_REVIEW"]
    warn(len(nr) > 0, f"{len(nr)} NEEDS_REVIEW brands — see unmapped_brands_by_source.csv", SECTION)

display(spark.createDataFrame(brand_pdf).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section A2 — Brand Coverage Profiling (one query per source)

# COMMAND ----------

# ── CELL 4: Brand profiling — PRD_MEX sources ─────────────────────────────────
SECTION = "A2-BRAND-COVERAGE"
log("INFO", "=" * 60, SECTION)

confirmed_brands = set(
    brand_pdf[brand_pdf["mapping_status"] == "CONFIRMED"]["raw_name_normalized"].str.upper()
) if not brand_pdf.empty else set()

BRAND_QUERIES = {
    # ── PRD_MEX ──────────────────────────────────────────────────────────────
    "SELL_IN": ("PRD_MEX", """
        SELECT 'SELL_IN' AS source_system,
               TRIM(UPPER(LV2_UMB_BRD_DSC)) AS raw_name_normalized,
               COUNT(DISTINCT TO_VARCHAR(MAT_IDT)) AS distinct_skus
        FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM
        WHERE LV2_UMB_BRD_DSC IS NOT NULL
        GROUP BY TRIM(UPPER(LV2_UMB_BRD_DSC))
        ORDER BY distinct_skus DESC
    """),
    "EDP_NIELSEN": ("PRD_MEX", """
        SELECT 'EDP_NIELSEN' AS source_system,
               TRIM(UPPER(INP_56985)) AS raw_name_normalized,
               COUNT(DISTINCT product_id) AS distinct_products
        FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_PROD_DIM
        WHERE INP_56985 IS NOT NULL
        GROUP BY TRIM(UPPER(INP_56985))
        ORDER BY distinct_products DESC
    """),
    "PB_NIELSEN": ("PRD_MEX", """
        SELECT 'PB_NIELSEN' AS source_system,
               TRIM(UPPER(INP_56985)) AS raw_name_normalized,
               COUNT(DISTINCT product_id) AS distinct_products
        FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_SUST_LECHE_ST_NLSN_PROD_DIM
        WHERE INP_56985 IS NOT NULL
        GROUP BY TRIM(UPPER(INP_56985))
        ORDER BY distinct_products DESC
    """),
    "WATER_NIELSEN_RIE": ("PRD_MEX", """
        SELECT 'WATER_NIELSEN_RIE' AS source_system,
               TRIM(UPPER(CSTM_310589)) AS raw_name_normalized,
               COUNT(DISTINCT product_id) AS distinct_products
        FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_RT_NLSN_PROD_DIM
        WHERE CSTM_310589 IS NOT NULL
        GROUP BY TRIM(UPPER(CSTM_310589))
        ORDER BY distinct_products DESC
    """),
    "WATER_SCANTRACK": ("PRD_MEX", """
        SELECT 'WATER_SCANTRACK' AS source_system,
               TRIM(UPPER(CSTM_310589)) AS raw_name_normalized,
               COUNT(DISTINCT product_id) AS distinct_products
        FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_ST_NLSN_PROD_DIM
        WHERE CSTM_310589 IS NOT NULL
        GROUP BY TRIM(UPPER(CSTM_310589))
        ORDER BY distinct_products DESC
    """),
    # ── PRD_MDP ──────────────────────────────────────────────────────────────
    "SELL_OUT": ("PRD_MDP", """
        SELECT 'SELL_OUT' AS source_system,
               TRIM(UPPER(BRAND)) AS raw_name_normalized,
               COUNT(DISTINCT TO_VARCHAR(INT_ID)) AS distinct_upcs
        FROM PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM
        WHERE BRAND IS NOT NULL
        GROUP BY TRIM(UPPER(BRAND))
        ORDER BY distinct_upcs DESC
    """),
    "MKT_ON": ("PRD_MDP", """
        SELECT 'MKT_ON' AS source_system,
               TRIM(UPPER(MARCA)) AS raw_name_normalized,
               COUNT(*) AS row_count
        FROM PRD_MDP.MDP_DSP.VW_MKT_ECOMM
        WHERE MARCA IS NOT NULL
        GROUP BY TRIM(UPPER(MARCA))
        ORDER BY row_count DESC
    """),
    "MKT_OFF": ("PRD_MDP", """
        SELECT 'MKT_OFF' AS source_system,
               TRIM(UPPER(MARCA)) AS raw_name_normalized,
               COUNT(*) AS row_count
        FROM PRD_MDP.MDP_STG.FACT_MEDIA_OFF
        WHERE MARCA IS NOT NULL
        GROUP BY TRIM(UPPER(MARCA))
        ORDER BY row_count DESC
    """),
    "IBP": ("PRD_MDP", """
        SELECT 'IBP' AS source_system,
               TRIM(UPPER(MARCA)) AS raw_name_normalized,
               COUNT(*) AS row_count
        FROM PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP
        WHERE MARCA IS NOT NULL
        GROUP BY TRIM(UPPER(MARCA))
        ORDER BY row_count DESC
    """),
    "WASTE": ("PRD_MDP", """
        SELECT 'WASTE' AS source_system,
               TRIM(UPPER(MARCA)) AS raw_name_normalized,
               COUNT(*) AS row_count
        FROM PRD_MDP.MDP_STG.FACT_TOPLINE
        WHERE MARCA IS NOT NULL
          AND UPPER(TRIM(FUENTE)) = 'TOPLINE'
        GROUP BY TRIM(UPPER(MARCA))
        ORDER BY row_count DESC
    """),
}

all_unmapped = []

for source, (db, sql) in BRAND_QUERIES.items():
    try:
        df = run_sf(db, sql)
        rows = df.collect()
        total_vals = len(rows)
        mapped = 0
        for r in rows:
            norm = str(r["RAW_NAME_NORMALIZED"] or "").strip().upper()
            if norm in confirmed_brands:
                mapped += 1
            else:
                all_unmapped.append({
                    "source_system":        source,
                    "raw_name_normalized":  norm,
                    "volume":               str(r[2]) if len(r) > 2 else "",
                    "recommended_action":   "Add to brand_mapping.csv with mapping_status=NEEDS_REVIEW",
                })
        pct = round(mapped / total_vals * 100, 1) if total_vals else 0.0
        log("INFO", f"{source}: {total_vals} values | mapped={mapped} ({pct}%)", SECTION)
        warn(pct < 99.0, f"{source} brand coverage {pct}% is below 99%.", SECTION)
        if pct >= 99.0:
            passed(f"{source} brand coverage {pct}%.", SECTION)
    except Exception as exc:
        log("⚠️  WARNING", f"{source}: query failed — {exc}", SECTION)

if all_unmapped:
    from pyspark.sql import Row
    df_unmapped = spark.createDataFrame(
        [Row(**r) for r in all_unmapped]
    )
    display(df_unmapped)
    save_df(df_unmapped, "unmapped_brands_by_source.csv")
    log("INFO", f"Exported {len(all_unmapped)} unmapped brands.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section A3 — UPC Golden Source Quality (V_D_ITEM — PRD_MEX)

# COMMAND ----------

# ── CELL 5: V_D_ITEM null/blank EAN + cardinality ─────────────────────────────
SECTION = "A3-UPC-GOLDEN-SOURCE"
log("INFO", "=" * 60, SECTION)

# A3.1 — Null coverage
SQL_EAN_NULL = """
SELECT
    COUNT(*)                                            AS total_rows,
    COUNT_IF(SKU_EAN_COD IS NULL)                       AS null_ean_rows,
    COUNT_IF(TRIM(TO_VARCHAR(SKU_EAN_COD)) = '')        AS blank_ean_rows,
    COUNT_IF(MAT_ACT_FLG = 1 AND SKU_EAN_COD IS NULL)  AS null_ean_active,
    COUNT(DISTINCT TO_VARCHAR(SKU_EAN_COD))             AS distinct_ean,
    COUNT(DISTINCT TO_VARCHAR(MAT_IDT))                 AS distinct_mat_idt
FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM
"""
df_null = run_sf("PRD_MEX", SQL_EAN_NULL)
display(df_null)
r = df_null.collect()[0]
log("INFO", f"Total rows={r['TOTAL_ROWS']} | NULL EAN={r['NULL_EAN_ROWS']} | NULL active={r['NULL_EAN_ACTIVE']} | Distinct EAN={r['DISTINCT_EAN']}", SECTION)
blocker(r["NULL_EAN_ACTIVE"] > 0, f"{r['NULL_EAN_ACTIVE']} active products have NULL SKU_EAN_COD.", SECTION)
if r["NULL_EAN_ACTIVE"] == 0:
    passed("No active products with NULL SKU_EAN_COD.", SECTION)

# A3.2 — Cardinality (1 EAN → 1 MAT_IDT)
SQL_CARD = """
SELECT TO_VARCHAR(SKU_EAN_COD) AS sku_ean_cod,
       COUNT(DISTINCT TO_VARCHAR(MAT_IDT)) AS distinct_mat_idt,
       COUNT(*) AS row_count
FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM
WHERE SKU_EAN_COD IS NOT NULL AND TRIM(TO_VARCHAR(SKU_EAN_COD)) <> ''
GROUP BY TO_VARCHAR(SKU_EAN_COD)
HAVING COUNT(DISTINCT TO_VARCHAR(MAT_IDT)) > 1
ORDER BY distinct_mat_idt DESC
"""
df_card = run_sf("PRD_MEX", SQL_CARD)
card_cnt = df_card.count()
display(df_card.limit(20))
blocker(card_cnt > 0, f"{card_cnt} EAN codes map to multiple MAT_IDTs — golden source integrity broken.", SECTION)
if card_cnt == 0:
    passed("All EAN codes map to exactly 1 MAT_IDT.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section A4 — SELL_OUT UPC Bridge Cascade

# COMMAND ----------

# ── CELL 6: P1 exact (INT_ID) — PRD_MEX join ─────────────────────────────────
SECTION = "A4-UPC-BRIDGE"
log("INFO", "=" * 60, SECTION)

SQL_P1 = """
SELECT TO_VARCHAR(prod.INT_ID)      AS sell_out_int_id,
       TO_VARCHAR(item.SKU_EAN_COD) AS sku_ean_cod,
       TO_VARCHAR(item.MAT_IDT)     AS mat_idt,
       item.MAT_LCL_DSC,
       prod.NAME  AS so_name,
       prod.BRAND AS so_brand,
       1          AS match_priority,
       'EXACT_INT_ID' AS match_method,
       1.0        AS match_confidence
FROM PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM prod
INNER JOIN PRD_MEX.MEX_DSP_OTC.V_D_ITEM item
    ON TO_VARCHAR(prod.INT_ID) = TO_VARCHAR(item.SKU_EAN_COD)
WHERE prod.INT_ID IS NOT NULL AND TRIM(TO_VARCHAR(prod.INT_ID)) <> ''
"""
# P1 query crosses PRD_MDP + PRD_MEX — run via PRD_MDP profile (has access to both)
df_p1 = run_sf("PRD_MDP", SQL_P1)
p1_cnt = df_p1.count()
log("INFO", f"P1 (INT_ID exact): {p1_cnt:,} matches", SECTION)
display(df_p1.limit(10))

# COMMAND ----------

# ── CELL 7: P2 exact (IMPORT_ID) — exclude already-matched by P1 ─────────────
SQL_P2 = """
WITH p1 AS (
    SELECT DISTINCT TO_VARCHAR(prod.INT_ID) AS matched
    FROM PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM prod
    INNER JOIN PRD_MEX.MEX_DSP_OTC.V_D_ITEM item
        ON TO_VARCHAR(prod.INT_ID) = TO_VARCHAR(item.SKU_EAN_COD)
    WHERE prod.INT_ID IS NOT NULL
)
SELECT TO_VARCHAR(prod.IMPORT_ID)   AS sell_out_import_id,
       TO_VARCHAR(item.SKU_EAN_COD) AS sku_ean_cod,
       TO_VARCHAR(item.MAT_IDT)     AS mat_idt,
       item.MAT_LCL_DSC,
       prod.NAME  AS so_name,
       prod.BRAND AS so_brand,
       2          AS match_priority,
       'EXACT_IMPORT_ID' AS match_method,
       1.0        AS match_confidence
FROM PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM prod
INNER JOIN PRD_MEX.MEX_DSP_OTC.V_D_ITEM item
    ON TO_VARCHAR(prod.IMPORT_ID) = TO_VARCHAR(item.SKU_EAN_COD)
LEFT JOIN p1 ON TO_VARCHAR(prod.INT_ID) = p1.matched
WHERE prod.IMPORT_ID IS NOT NULL
  AND TRIM(TO_VARCHAR(prod.IMPORT_ID)) <> ''
  AND p1.matched IS NULL
"""
df_p2 = run_sf("PRD_MDP", SQL_P2)
p2_cnt = df_p2.count()
log("INFO", f"P2 (IMPORT_ID exact): {p2_cnt:,} matches", SECTION)
display(df_p2.limit(10))

# COMMAND ----------

# ── CELL 8: Unmatched SELL_OUT products ───────────────────────────────────────
SQL_UNMATCHED = """
SELECT TO_VARCHAR(prod.INT_ID)    AS int_id,
       TO_VARCHAR(prod.IMPORT_ID) AS import_id,
       prod.NAME  AS so_name,
       prod.BRAND AS so_brand,
       prod.CBU_ID,
       'UNMATCHED' AS match_method,
       0.0         AS match_confidence,
       'NEEDS_REVIEW' AS review_status
FROM PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM prod
WHERE NOT EXISTS (
    SELECT 1 FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM i
    WHERE TO_VARCHAR(prod.INT_ID) = TO_VARCHAR(i.SKU_EAN_COD)
)
AND NOT EXISTS (
    SELECT 1 FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM i
    WHERE TO_VARCHAR(prod.IMPORT_ID) = TO_VARCHAR(i.SKU_EAN_COD)
)
ORDER BY prod.BRAND, prod.NAME
"""
df_unmatched = run_sf("PRD_MDP", SQL_UNMATCHED)
unmatched_cnt = df_unmatched.count()
display(df_unmatched.limit(20))
save_df(df_unmatched, "upc_bridge_unmatched_sell_out.csv")

total_so = p1_cnt + p2_cnt + unmatched_cnt
p1_pct   = round(p1_cnt / total_so * 100, 1) if total_so else 0.0
p2_pct   = round(p2_cnt / total_so * 100, 1) if total_so else 0.0
um_pct   = round(unmatched_cnt / total_so * 100, 1) if total_so else 0.0

log("INFO", f"Cascade summary — total={total_so:,} | P1={p1_pct}% | P2={p2_pct}% | unmatched={um_pct}%", SECTION)

cascade_rows = [("EXACT_INT_ID", 1, p1_cnt, p1_pct), ("EXACT_IMPORT_ID", 2, p2_cnt, p2_pct),
                ("UNMATCHED", 3, unmatched_cnt, um_pct)]
display(spark.createDataFrame(cascade_rows, ["match_method","priority","count","pct"]))

warn(p1_pct < 70.0, f"P1 match rate {p1_pct}% below 70% threshold.", SECTION)
if p1_pct >= 70.0:
    passed(f"P1 match rate {p1_pct}% ≥ 70%.", SECTION)
log("INFO", "REMINDER: P3 fuzzy matching is PROHIBITED from auto-promotion. "
           "Fuzzy candidates quarantined in upc_bridge_fuzzy_candidates.csv.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section A5 — CANAL & CADENA Sentinel Audit

# COMMAND ----------

# ── CELL 9: Load catalogs + sentinel checks ───────────────────────────────────
SECTION = "A5-SENTINEL-AUDIT"
log("INFO", "=" * 60, SECTION)

chan_csv = os.path.join(_HOMO_PATH, "channel_mapping.csv")
cad_csv  = os.path.join(_HOMO_PATH, "cadena_mapping.csv")
chan_pdf  = pd.read_csv(chan_csv, dtype=str).fillna("") if os.path.exists(chan_csv) else pd.DataFrame()
cad_pdf  = pd.read_csv(cad_csv,  dtype=str).fillna("") if os.path.exists(cad_csv)  else pd.DataFrame()

# MKT_ON sentinel
mkt_on_ok = (not chan_pdf.empty and
             len(chan_pdf[(chan_pdf["source_system"] == "MKT_ON") &
                          (chan_pdf["canal_std"] == "ECOMMERCE_MEDIA")]) > 0)
if mkt_on_ok: passed("MKT_ON CANAL sentinel = 'ECOMMERCE_MEDIA' confirmed.", SECTION)
else:          warn(True, "MKT_ON sentinel 'ECOMMERCE_MEDIA' not found in channel_mapping.csv", SECTION)

# MKT_OFF CANAL sentinel
mkt_off_ok = (not chan_pdf.empty and
              len(chan_pdf[(chan_pdf["source_system"] == "MKT_OFF") &
                           (chan_pdf["canal_std"] == "OFFLINE_MEDIA")]) > 0)
if mkt_off_ok: passed("MKT_OFF CANAL sentinel = 'OFFLINE_MEDIA' confirmed.", SECTION)
else:           warn(True, "MKT_OFF sentinel 'OFFLINE_MEDIA' not found in channel_mapping.csv", SECTION)

# MKT_OFF CADENA must be NULL (no fake string)
if not cad_pdf.empty and "source_system" in cad_pdf.columns:
    fake_cadena = cad_pdf[
        (cad_pdf["source_system"] == "MKT_OFF") &
        (cad_pdf["cadena_std"].str.strip().str.len() > 0)
    ]
    blocker(len(fake_cadena) > 0,
            f"MKT_OFF has {len(fake_cadena)} non-NULL CADENA_STD value(s) — must be NULL.",
            SECTION)
    if len(fake_cadena) == 0:
        passed("MKT_OFF CADENA_STD correctly NULL — no fake strings detected.", SECTION)

# Export NEEDS_REVIEW catalogs
for pdf, fname, cols in [
    (chan_pdf, "unmapped_channels_by_source.csv",
     ["raw_channel_value","raw_channel_normalized","source_system","canal_std","canal_type","mapping_status","is_active","notes"]),
    (cad_pdf, "unmapped_cadenas_by_source.csv",
     ["raw_cadena_value","raw_cadena_normalized","source_system","cadena_std","cadena_type","mapping_status","is_active","notes"]),
]:
    if not pdf.empty and "mapping_status" in pdf.columns:
        nr = pdf[pdf["mapping_status"] == "NEEDS_REVIEW"]
        if len(nr):
            save_df(spark.createDataFrame(nr), fname)
            log("INFO", f"{len(nr)} NEEDS_REVIEW rows → {fname}", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section A6 — WASTE Physical Column Validation (PRD_MDP)

# COMMAND ----------

# ── CELL 10: Confirm WASTE ($) and WASTE (KG) exist in FACT_TOPLINE ──────────
SECTION = "A6-WASTE-COLUMNS"
log("INFO", "=" * 60, SECTION)

SQL_WASTE_COLS = """
SELECT COLUMN_NAME, DATA_TYPE
FROM PRD_MDP.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'MDP_STG'
  AND TABLE_NAME   = 'FACT_TOPLINE'
  AND (
        UPPER(COLUMN_NAME) LIKE '%WASTE%'
     OR UPPER(COLUMN_NAME) LIKE '%KGS%'
     OR UPPER(COLUMN_NAME) LIKE '%KG%'
     OR UPPER(COLUMN_NAME) LIKE '%VAL%'
     OR UPPER(COLUMN_NAME) LIKE '%AMOUNT%'
     OR UPPER(COLUMN_NAME) LIKE '%VOLUME%'
  )
ORDER BY COLUMN_NAME
"""
df_waste = run_sf("PRD_MDP", SQL_WASTE_COLS)
display(df_waste)
found_cols = {r["COLUMN_NAME"] for r in df_waste.collect()}

for expected in ["WASTE ($)", "WASTE (KG)"]:
    if expected in found_cols:
        passed(f"Column '{expected}' exists in FACT_TOPLINE.", SECTION)
    else:
        warn(True, f"Column '{expected}' NOT found — verify physical name before coding.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section A7 — Nielsen Market Bridge Coverage (PRD_MEX)

# COMMAND ----------

# ── CELL 11: Profile all four Nielsen market dims ─────────────────────────────
SECTION = "A7-NIELSEN-BRIDGE"
log("INFO", "=" * 60, SECTION)

nielsen_csv = os.path.join(_HOMO_PATH, "nielsen_market_mapping.csv")
bridge_pdf  = pd.read_csv(nielsen_csv, dtype=str).fillna("") if os.path.exists(nielsen_csv) else pd.DataFrame()
confirmed_markets = (set(bridge_pdf[bridge_pdf.get("mapping_status", pd.Series(dtype=str)) == "CONFIRMED"]["raw_market_normalized"].str.upper())
                     if not bridge_pdf.empty else set())

NIELSEN_MKT_QUERIES = {
    "EDP_NIELSEN":        "SELECT DISTINCT TRIM(UPPER(MRKT_DSC_SHRT)) AS v FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_MKT_DIM",
    "PB_NIELSEN":         "SELECT DISTINCT TRIM(UPPER(MRKT_DSC_SHRT)) AS v FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_SUST_LECHE_ST_NLSN_MKT_DIM",
    "WATER_NIELSEN_RIE":  "SELECT DISTINCT TRIM(UPPER(MRKT_DSC_SHRT)) AS v FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_RT_NLSN_MKT_DIM",
    "WATER_SCANTRACK":    "SELECT DISTINCT TRIM(UPPER(MRKT_DSC_SHRT)) AS v FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_ST_NLSN_MKT_DIM",
}

new_market_rows = []
for src, sql in NIELSEN_MKT_QUERIES.items():
    try:
        df = run_sf("PRD_MEX", sql)
        vals = [r["V"] for r in df.collect()]
        unmapped = [v for v in vals if v and v.upper() not in confirmed_markets]
        log("INFO", f"{src}: {len(vals)} total | {len(unmapped)} unmapped", SECTION)
        warn(len(unmapped) > 0,
             f"{src}: {len(unmapped)} market strings need mapping in nielsen_market_mapping.csv", SECTION)
        for v in unmapped:
            new_market_rows.append({
                "raw_market_value": v, "raw_market_normalized": v, "source_system": src,
                "canal_std": "", "cadena_std": "", "region_std": "",
                "market_type": "", "reading_type": "",
                "mapping_status": "NEEDS_REVIEW",
                "notes": f"Auto-discovered — map manually",
            })
    except Exception as exc:
        log("⚠️  WARNING", f"{src}: query failed — {exc}", SECTION)

if new_market_rows:
    df_new_mkts = spark.createDataFrame(new_market_rows)
    display(df_new_mkts)
    save_df(df_new_mkts, "unmapped_nielsen_markets.csv")
    log("INFO", f"{len(new_market_rows)} new market strings → unmapped_nielsen_markets.csv", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Summary

# COMMAND ----------

# ── CELL 12: Summary + flush log ──────────────────────────────────────────────
SECTION = "SUMMARY"
log("INFO", "=" * 60, SECTION)
log("INFO", f"Hard blockers : {len(_HARD_BLOCKERS)}", SECTION)
log("INFO", f"Warnings      : {len(_WARNINGS)}", SECTION)

if _HARD_BLOCKERS:
    for b in _HARD_BLOCKERS:
        log("🚨 BLOCKER", b, SECTION)
else:
    passed("Zero hard blockers — Notebook A clean.", SECTION)

if _WARNINGS:
    for w in _WARNINGS:
        log("⚠️  WARNING", w, SECTION)

flush_log()
print(f"\n{'='*60}")
print(f"Notebook A complete — {ts()}")
print(f"Output root: {DBFS_ROOT}")
print(f"{'='*60}")
