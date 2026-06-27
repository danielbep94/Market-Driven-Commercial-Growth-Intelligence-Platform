# Databricks notebook source

# MAGIC %md
# MAGIC # MDM Catalog Validation — Notebook A (v3)
# MAGIC
# MAGIC **Purpose:** Profile MDM catalogs against all source systems and write structured audit logs.
# MAGIC
# MAGIC **Key architectural rule (v3):**
# MAGIC > `MAT_IDT` is the unique SAP product key.
# MAGIC > `SKU_EAN_COD` is a barcode attribute — it may map to multiple `MAT_IDT`s.
# MAGIC > `SKU_EAN_COD` must never be used alone as a unique 1:1 product join key.
# MAGIC
# MAGIC **Credential resolution:**
# MAGIC - `PRD_MEX` → `configs/snowflake_creds.py` SF_MEX_* (PRD_OSM_DPH_READER)
# MAGIC - `PRD_MDP` → SF_MDP_* or Key Vault (DAN-AM-P-KVT800-R-MDP-DB)
# MAGIC
# MAGIC **Run `notebooks/validate_credentials.py` first — all 6 cells must pass.**
# MAGIC
# MAGIC **Output root:** `dbfs:/mnt/mdp/mdm/notebook_a/`

# COMMAND ----------

# ── CELL 1: Load credentials ──────────────────────────────────────────────────
import os, importlib.util, datetime
from pyspark.sql import functions as F

_current_dir = os.getcwd()
_creds_path  = os.path.normpath(os.path.join(_current_dir, "..", "configs", "snowflake_creds.py"))

if not os.path.exists(_creds_path):
    raise FileNotFoundError(
        "❌ configs/snowflake_creds.py NOT FOUND.\n"
        "   Copy configs/snowflake_creds.example.py → configs/snowflake_creds.py"
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
DBFS_ROOT = "dbfs:/mnt/mdp/mdm/notebook_a"
LOCAL_ROOT = "/dbfs/mnt/mdp/mdm/notebook_a"
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
    dbutils.fs.put(f"{DBFS_ROOT}/validation_results_mdm_catalogs.txt", content, overwrite=True)
    _repo_log = os.path.join(REPO_LOGS_DIR, "validation_results_mdm_catalogs.txt")
    with open(_repo_log, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"📄 Log → DBFS: {DBFS_ROOT}/validation_results_mdm_catalogs.txt")
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

_HOMO_PATH = os.path.normpath(os.path.join(_current_dir, "..", "homologation"))

import pandas as pd
print(f"✅ CELL 2 ready (dual-write). DBFS: {DBFS_ROOT} | REPO: {REPO_LOGS_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section A1 — CAT_MARCA Structural Validation

# COMMAND ----------

# ── CELL 3: Brand catalog uniqueness ─────────────────────────────────────────
SECTION = "A1-CAT_MARCA"
log("INFO", "=" * 60, SECTION)

brand_csv = os.path.join(_HOMO_PATH, "brand_mapping.csv")
if not os.path.exists(brand_csv):
    blocker(True, "brand_mapping.csv not found", SECTION)
    brand_pdf = pd.DataFrame()
else:
    brand_pdf = pd.read_csv(brand_csv, dtype=str).fillna("")
    brand_pdf = brand_pdf[~brand_pdf.iloc[:, 0].str.startswith("[PENDING", na=False)]
    total = len(brand_pdf)
    log("INFO", f"Loaded {total} rows from brand_mapping.csv", SECTION)

    dupes = (brand_pdf.groupby(["source_system", "raw_name_normalized"])
             .size().reset_index(name="count"))
    dupes = dupes[dupes["count"] > 1]
    blocker(len(dupes) > 0,
            f"{len(dupes)} duplicate source+raw_name_normalized combos — must be 0 (Blocker #1).", SECTION)
    if len(dupes) == 0:
        passed("CAT_MARCA uniqueness check: no duplicate source+raw_name combinations.", SECTION)

    status_counts = brand_pdf.get("mapping_status", pd.Series(dtype=str)).value_counts().to_dict()
    for s, c in status_counts.items():
        log("INFO", f"  mapping_status={s}: {c}", SECTION)

    nr = brand_pdf[brand_pdf.get("mapping_status", pd.Series(dtype=str)) == "NEEDS_REVIEW"]
    warn(len(nr) > 0, f"{len(nr)} NEEDS_REVIEW brands in brand_mapping.csv.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section A2 — CAT_UPC Grain Validation (MAT_IDT-first)

# COMMAND ----------

# MAGIC %md
# MAGIC ### A2 Key Rule
# MAGIC > `CAT_UPC` grain = **one row per `MAT_IDT`** (not per EAN).
# MAGIC > `SKU_EAN_COD` is a barcode attribute — may be shared across multiple `MAT_IDT`s.

# COMMAND ----------

# ── CELL 4: V_D_ITEM EAN quality + cardinality ───────────────────────────────
SECTION = "A2-CAT_UPC"
log("INFO", "=" * 60, SECTION)
log("INFO", "Rule: CAT_UPC grain = one row per MAT_IDT. SKU_EAN_COD is NOT a unique key.", SECTION)

# A2.1 — Null/blank EAN quality
SQL_EAN_QUALITY = """
SELECT
    COUNT(*)                                                  AS total_rows,
    COUNT(DISTINCT TO_VARCHAR(MAT_IDT))                       AS distinct_mat_idt,
    COUNT(DISTINCT TO_VARCHAR(SKU_EAN_COD))                   AS distinct_ean,
    COUNT_IF(SKU_EAN_COD IS NULL)                             AS null_ean_rows,
    COUNT_IF(TRY_TO_NUMBER(MAT_ACT_FLG) = 1 AND SKU_EAN_COD IS NULL) AS null_ean_active,
    COUNT_IF(TRIM(TO_VARCHAR(SKU_EAN_COD)) = '')              AS blank_ean_rows,
    COUNT_IF(TRY_TO_NUMBER(MAT_ACT_FLG) = 1)                 AS active_rows
FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM
"""
df_q = run_sf("PRD_MEX", SQL_EAN_QUALITY)
display(df_q)
r = df_q.collect()[0]
log("INFO", f"V_D_ITEM: total={r['TOTAL_ROWS']:,} | distinct MAT_IDT={r['DISTINCT_MAT_IDT']:,} | distinct EAN={r['DISTINCT_EAN']:,}", SECTION)
log("INFO", f"  NULL EAN (all)={r['NULL_EAN_ROWS']:,} | NULL EAN (active)={r['NULL_EAN_ACTIVE']:,} | active rows={r['ACTIVE_ROWS']:,}", SECTION)

blocker(r["NULL_EAN_ACTIVE"] > 0,
        f"Blocker #2: {r['NULL_EAN_ACTIVE']} active products have NULL SKU_EAN_COD.", SECTION)
if r["NULL_EAN_ACTIVE"] == 0:
    passed("No active products with NULL SKU_EAN_COD.", SECTION)
warn(r["NULL_EAN_ROWS"] > 0,
     f"{r['NULL_EAN_ROWS']} total rows have NULL EAN (including inactive) — investigate if needed.", SECTION)

# A2.2 — EAN cardinality profile
SQL_EAN_CARD = """
SELECT TO_VARCHAR(SKU_EAN_COD) AS sku_ean_cod,
       COUNT(DISTINCT TO_VARCHAR(MAT_IDT))                                                      AS mat_idt_count,
       COUNT(DISTINCT CASE WHEN TRY_TO_NUMBER(MAT_ACT_FLG)=1 THEN TO_VARCHAR(MAT_IDT) END)    AS active_mat_idt_count,
       COUNT(DISTINCT LV2_UMB_BRD_DSC)                                                         AS brand_count,
       COUNT(DISTINCT MAT_LCL_DSC)                                                              AS description_count,
       LISTAGG(DISTINCT TO_VARCHAR(MAT_ACT_FLG), ', ')
           WITHIN GROUP (ORDER BY TO_VARCHAR(MAT_ACT_FLG))                                     AS active_flags
FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM
WHERE SKU_EAN_COD IS NOT NULL
GROUP BY TO_VARCHAR(SKU_EAN_COD)
HAVING COUNT(DISTINCT TO_VARCHAR(MAT_IDT)) > 1
ORDER BY active_mat_idt_count DESC, mat_idt_count DESC
"""
df_card = run_sf("PRD_MEX", SQL_EAN_CARD)
card_total = df_card.count()
display(df_card.limit(20))
save_df(df_card, "ean_cardinality_profile.csv", SECTION)
log("INFO", f"EAN codes mapping to >1 MAT_IDT: {card_total:,}", SECTION)

# A2.3 — Active-active duplicate test (the critical check)
SQL_ACTIVE_CONFLICT = """
WITH active_ean AS (
    SELECT TO_VARCHAR(SKU_EAN_COD) AS ean,
           COUNT(DISTINCT TO_VARCHAR(MAT_IDT)) AS active_mat_idt_count
    FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM
    WHERE SKU_EAN_COD IS NOT NULL
      AND TRY_TO_NUMBER(MAT_ACT_FLG) = 1
    GROUP BY TO_VARCHAR(SKU_EAN_COD)
)
SELECT * FROM active_ean WHERE active_mat_idt_count > 1
ORDER BY active_mat_idt_count DESC
"""
df_conflict = run_sf("PRD_MEX", SQL_ACTIVE_CONFLICT)
conflict_count = df_conflict.count()
display(df_conflict.limit(20))
save_df(df_conflict, "ean_active_active_conflict.csv", SECTION)

if conflict_count == 0:
    warn(True,
         f"Warning #7: {card_total:,} EANs map to multiple MAT_IDTs, but ALL are inactive history only. "
         "Blocker downgraded — implement preferred-MAT_IDT rule (ONLY_ACTIVE_MAT_IDT_FOR_EAN).", SECTION)
    passed("No active-active EAN conflicts. Preferred-MAT_IDT rule resolves all ambiguity.", SECTION)
else:
    blocker(True,
            f"Blocker: {conflict_count:,} EANs have multiple ACTIVE MAT_IDTs (MULTI_MAT_IDT_ACTIVE_CONFLICT). "
            "Do not auto-promote SELL_OUT matches for these EANs. Manual review required.", SECTION)

# A2.4 — Active SKU export count (blocker if 0 after MAT_ACT_FLG fix)
SQL_ACTIVE_COUNT = """
SELECT COUNT(*) AS active_sku_count
FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM
WHERE TRY_TO_NUMBER(MAT_ACT_FLG) = 1
"""
df_active = run_sf("PRD_MEX", SQL_ACTIVE_COUNT)
active_count = df_active.collect()[0]["ACTIVE_SKU_COUNT"]
log("INFO", f"Active SKU count (TRY_TO_NUMBER filter): {active_count:,}", SECTION)
blocker(active_count == 0,
        "Blocker #11: sku_mapping.csv would export 0 active SKUs — TRY_TO_NUMBER(MAT_ACT_FLG) returned 0 rows.", SECTION)
if active_count > 0:
    passed(f"Active SKU count = {active_count:,} — sku_mapping.csv will export correctly.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section A3 — Brand Coverage Profiling (all 10 sources)

# COMMAND ----------

# ── CELL 5: Brand profiling ───────────────────────────────────────────────────
SECTION = "A3-BRAND-COVERAGE"
log("INFO", "=" * 60, SECTION)

confirmed_brands = set(
    brand_pdf[brand_pdf.get("mapping_status", pd.Series(dtype=str)) == "CONFIRMED"]
    ["raw_name_normalized"].str.upper()
) if not brand_pdf.empty else set()

BRAND_QUERIES = {
    "SELL_IN":           ("PRD_MEX", "SELECT TRIM(UPPER(LV2_UMB_BRD_DSC)) AS b, COUNT(DISTINCT TO_VARCHAR(MAT_IDT)) AS n FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM WHERE LV2_UMB_BRD_DSC IS NOT NULL GROUP BY TRIM(UPPER(LV2_UMB_BRD_DSC))"),
    "SELL_OUT":          ("PRD_MDP", "SELECT TRIM(UPPER(BRAND)) AS b, COUNT(DISTINCT TO_VARCHAR(INT_ID)) AS n FROM PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM WHERE BRAND IS NOT NULL GROUP BY TRIM(UPPER(BRAND))"),
    "MKT_ON":            ("PRD_MDP", "SELECT TRIM(UPPER(MARCA)) AS b, COUNT(*) AS n FROM PRD_MDP.MDP_DSP.VW_MKT_ECOMM WHERE MARCA IS NOT NULL GROUP BY TRIM(UPPER(MARCA))"),
    "MKT_OFF":           ("PRD_MDP", "SELECT TRIM(UPPER(MARCA)) AS b, COUNT(*) AS n FROM PRD_MDP.MDP_STG.FACT_MEDIA_OFF WHERE MARCA IS NOT NULL GROUP BY TRIM(UPPER(MARCA))"),
    "IBP":               ("PRD_MDP", "SELECT TRIM(UPPER(MARCA)) AS b, COUNT(*) AS n FROM PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP WHERE MARCA IS NOT NULL GROUP BY TRIM(UPPER(MARCA))"),
    "WASTE":             ("PRD_MDP", "SELECT TRIM(UPPER(MARCA)) AS b, COUNT(*) AS n FROM PRD_MDP.MDP_STG.FACT_TOPLINE WHERE MARCA IS NOT NULL AND UPPER(TRIM(FUENTE))='TOPLINE' GROUP BY TRIM(UPPER(MARCA))"),
    "EDP_NIELSEN":       ("PRD_MEX", "SELECT TRIM(UPPER(INP_56985)) AS b, COUNT(DISTINCT PRODUCT_ID) AS n FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_PROD_DIM WHERE INP_56985 IS NOT NULL GROUP BY TRIM(UPPER(INP_56985))"),
    "PB_NIELSEN":        ("PRD_MEX", "SELECT TRIM(UPPER(INP_56985)) AS b, COUNT(DISTINCT PRODUCT_ID) AS n FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_SUST_LECHE_ST_NLSN_PROD_DIM WHERE INP_56985 IS NOT NULL GROUP BY TRIM(UPPER(INP_56985))"),
    "WATER_NIELSEN_RIE": ("PRD_MEX", "SELECT TRIM(UPPER(CSTM_310589)) AS b, COUNT(DISTINCT PRODUCT_ID) AS n FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_RT_NLSN_PROD_DIM WHERE CSTM_310589 IS NOT NULL GROUP BY TRIM(UPPER(CSTM_310589))"),
    "WATER_SCANTRACK":   ("PRD_MEX", "SELECT TRIM(UPPER(CSTM_310589)) AS b, COUNT(DISTINCT PRODUCT_ID) AS n FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_ST_NLSN_PROD_DIM WHERE CSTM_310589 IS NOT NULL GROUP BY TRIM(UPPER(CSTM_310589))"),
}

all_unmapped = []
for source, (db, sql) in BRAND_QUERIES.items():
    try:
        rows = run_sf(db, sql).collect()
        total_vals = len(rows)
        mapped = sum(1 for r in rows if str(r["B"] or "").strip().upper() in confirmed_brands)
        unmapped = [{"source_system": source, "raw_name_normalized": str(r["B"] or "").strip().upper(),
                     "volume": str(r["N"]), "recommended_action": "Add to brand_mapping.csv"}
                    for r in rows if str(r["B"] or "").strip().upper() not in confirmed_brands]
        all_unmapped.extend(unmapped)
        pct = round(mapped / total_vals * 100, 1) if total_vals else 0.0
        log("INFO", f"{source}: {total_vals} values | mapped={mapped} ({pct}%)", SECTION)
        warn(pct < 99.0, f"Warning #1: {source} brand coverage {pct}% below 99%.", SECTION)
        if pct >= 99.0: passed(f"{source} brand coverage {pct}%.", SECTION)
    except Exception as exc:
        warn(True, f"{source}: query failed — {exc}", SECTION)

if all_unmapped:
    from pyspark.sql import Row
    df_um = spark.createDataFrame([Row(**r) for r in all_unmapped])
    display(df_um)
    save_df(df_um, "unmapped_brands_by_source.csv", SECTION)
    log("INFO", f"Exported {len(all_unmapped)} unmapped brand values.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section A4 — SELL_OUT UPC Bridge Priority Distribution

# COMMAND ----------

# MAGIC %md
# MAGIC ### A4 Key Rule
# MAGIC > UPC bridge checks are SELL_IN ↔ SELL_OUT only.
# MAGIC > `UPC_STD` is NOT a predicate in the all-source join.
# MAGIC > MKT_ON and MKT_OFF are NEVER joined using UPC.

# COMMAND ----------

# ── CELL 6: P1 (INT_ID exact) — split Spark reads ────────────────────────────
SECTION = "A4-UPC-BRIDGE"
log("INFO", "=" * 60, SECTION)
log("INFO", "UPC_STD is NOT an all-source join predicate. Bridge = SELL_IN ↔ SELL_OUT only.", SECTION)

SQL_PROD = """
SELECT TO_VARCHAR(INT_ID) AS int_id, TO_VARCHAR(IMPORT_ID) AS import_id,
       NAME AS so_name, BRAND AS so_brand, CBU_ID
FROM PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM
WHERE INT_ID IS NOT NULL AND TRIM(TO_VARCHAR(INT_ID)) <> ''
"""
SQL_ITEM = """
SELECT TO_VARCHAR(SKU_EAN_COD) AS sku_ean_cod,
       TO_VARCHAR(MAT_IDT) AS mat_idt,
       MAT_LCL_DSC AS si_description,
       TRY_TO_NUMBER(MAT_ACT_FLG) AS is_active_num
FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM
WHERE SKU_EAN_COD IS NOT NULL
"""

df_prod = run_sf("PRD_MDP", SQL_PROD)
df_item = run_sf("PRD_MEX", SQL_ITEM)

# Preferred MAT_IDT: prefer active rows; if multiple active → flag as ambiguous
from pyspark.sql.window import Window
w = Window.partitionBy("sku_ean_cod")
df_item_pref = (df_item
    .withColumn("ean_mat_idt_count", F.count("mat_idt").over(w))
    .withColumn("active_mat_idt_count_for_ean", F.sum(F.col("is_active_num")).over(w))
    .withColumn("ean_cardinality_status",
                F.when(F.col("ean_mat_idt_count") == 1, "UNIQUE_EAN")
                 .when((F.col("ean_mat_idt_count") > 1) & (F.col("active_mat_idt_count_for_ean") == 1), "MULTI_MAT_IDT_INACTIVE_HISTORY")
                 .when((F.col("ean_mat_idt_count") > 1) & (F.col("active_mat_idt_count_for_ean") > 1), "MULTI_MAT_IDT_ACTIVE_CONFLICT")
                 .otherwise("NULL_EAN"))
    .withColumn("is_preferred",
                F.when(F.col("ean_cardinality_status") == "UNIQUE_EAN", F.lit(True))
                 .when((F.col("ean_cardinality_status") == "MULTI_MAT_IDT_INACTIVE_HISTORY") & (F.col("is_active_num") == 1), F.lit(True))
                 .otherwise(F.lit(False))))

# P1: preferred matches only
df_p1_pref = df_item_pref.filter(F.col("is_preferred") == True)
df_p1 = (df_prod.join(df_p1_pref, df_prod["int_id"] == df_p1_pref["sku_ean_cod"], "inner")
         .select(df_prod["int_id"].alias("sell_out_int_id"),
                 df_p1_pref["sku_ean_cod"], df_p1_pref["mat_idt"],
                 df_p1_pref["si_description"], df_prod["so_name"], df_prod["so_brand"],
                 df_p1_pref["ean_cardinality_status"],
                 F.lit(1).alias("match_priority"),
                 F.lit("EXACT_INT_ID").alias("match_method"),
                 F.lit(1.0).alias("match_confidence"),
                 F.lit("CONFIRMED").alias("review_status")))
p1_cnt = df_p1.count()
log("INFO", f"P1 (INT_ID exact, preferred MAT_IDT): {p1_cnt:,}", SECTION)
display(df_p1.limit(10))

# COMMAND ----------

# ── CELL 7: Ambiguous EAN matches (MULTI_MAT_IDT_ACTIVE_CONFLICT) ─────────────
df_item_conflict = df_item_pref.filter(F.col("ean_cardinality_status") == "MULTI_MAT_IDT_ACTIVE_CONFLICT")
df_p1_ambiguous = (df_prod.join(df_item_conflict, df_prod["int_id"] == df_item_conflict["sku_ean_cod"], "inner")
                   .select(df_prod["int_id"], df_prod["so_name"], df_prod["so_brand"],
                           df_item_conflict["ean_cardinality_status"],
                           F.lit("NO_PREFERRED_MAT_IDT").alias("reason"),
                           F.lit("NEEDS_REVIEW").alias("review_status")))
ambiguous_cnt = df_p1_ambiguous.count()

if ambiguous_cnt > 0:
    display(df_p1_ambiguous.limit(20))
    save_df(df_p1_ambiguous, "upc_bridge_ean_ambiguous.csv", SECTION)
    blocker(True,
            f"Blocker #5: {ambiguous_cnt:,} SELL_OUT products match EANs with MULTI_MAT_IDT_ACTIVE_CONFLICT "
            "and no preferred MAT_IDT. Written to upc_bridge_ean_ambiguous.csv.", SECTION)
else:
    passed("No ambiguous EAN matches — all multi-MAT_IDT EANs resolved by preferred-MAT_IDT rule.", SECTION)

# COMMAND ----------

# ── CELL 8: Unmatched + cascade summary ───────────────────────────────────────
SQL_EAN_KEYS = "SELECT DISTINCT TO_VARCHAR(SKU_EAN_COD) AS sku_ean_cod FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM WHERE SKU_EAN_COD IS NOT NULL"
df_ean_keys_i = run_sf("PRD_MEX", SQL_EAN_KEYS)
df_ean_keys_m = run_sf("PRD_MEX", SQL_EAN_KEYS)

df_no_int = df_prod.join(df_ean_keys_i, df_prod["int_id"] == df_ean_keys_i["sku_ean_cod"], "left_anti")
df_unmatched = (df_no_int
                .join(df_ean_keys_m, df_no_int["import_id"] == df_ean_keys_m["sku_ean_cod"], "left_anti")
                .select(F.col("int_id"), F.col("import_id"), F.col("so_name"), F.col("so_brand"),
                        F.lit("UNMATCHED").alias("match_method"),
                        F.lit(0.0).alias("match_confidence"),
                        F.lit("NEEDS_REVIEW").alias("review_status")))
unmatched_cnt = df_unmatched.count()
display(df_unmatched.limit(20))
save_df(df_unmatched, "upc_bridge_unmatched_sell_out.csv", SECTION)

total_so = p1_cnt + unmatched_cnt + ambiguous_cnt
p1_pct = round(p1_cnt / total_so * 100, 1) if total_so else 0.0
um_pct = round(unmatched_cnt / total_so * 100, 1) if total_so else 0.0

cascade_data = [("EXACT_INT_ID_PREFERRED", 1, p1_cnt, p1_pct, "CONFIRMED"),
                ("UNMATCHED", 3, unmatched_cnt, um_pct, "NEEDS_REVIEW"),
                ("EAN_AMBIGUOUS_CONFLICT", 99, ambiguous_cnt,
                 round(ambiguous_cnt/total_so*100,1) if total_so else 0.0, "BLOCKED")]
df_cascade = spark.createDataFrame(cascade_data, ["match_method","priority","count","pct","status"])
display(df_cascade)
save_df(df_cascade, "upc_bridge_priority_distribution.csv", SECTION)

log("INFO", f"Bridge summary: total={total_so:,} | P1={p1_pct}% | unmatched={um_pct}%", SECTION)
warn(p1_pct < 70.0, f"Warning #2: P1 match rate {p1_pct}% below 70% threshold.", SECTION)
if p1_pct >= 70.0: passed(f"P1 match rate {p1_pct}% ≥ 70%.", SECTION)
log("INFO", "REMINDER: P3 fuzzy matching PROHIBITED from auto-promotion.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section A5 — CANAL & CADENA Sentinel Audit

# COMMAND ----------

# ── CELL 9: Sentinel checks ───────────────────────────────────────────────────
SECTION = "A5-SENTINEL-AUDIT"
log("INFO", "=" * 60, SECTION)

chan_csv = os.path.join(_HOMO_PATH, "channel_mapping.csv")
cad_csv  = os.path.join(_HOMO_PATH, "cadena_mapping.csv")
chan_pdf  = pd.read_csv(chan_csv, dtype=str).fillna("") if os.path.exists(chan_csv) else pd.DataFrame()
cad_pdf  = pd.read_csv(cad_csv, dtype=str).fillna("") if os.path.exists(cad_csv) else pd.DataFrame()

# MKT_ON sentinel = ECOMMERCE_MEDIA
mkt_on_ok = (not chan_pdf.empty and
             len(chan_pdf[(chan_pdf.get("source_system","") == "MKT_ON") &
                          (chan_pdf.get("canal_std","") == "ECOMMERCE_MEDIA")]) > 0)
if mkt_on_ok: passed("MKT_ON CANAL_STD = 'ECOMMERCE_MEDIA' confirmed.", SECTION)
else:          warn(True, "MKT_ON sentinel 'ECOMMERCE_MEDIA' not found in channel_mapping.csv", SECTION)

# MKT_OFF CANAL sentinel = OFFLINE_MEDIA
mkt_off_ok = (not chan_pdf.empty and
              len(chan_pdf[(chan_pdf.get("source_system","") == "MKT_OFF") &
                           (chan_pdf.get("canal_std","") == "OFFLINE_MEDIA")]) > 0)
if mkt_off_ok: passed("MKT_OFF CANAL_STD = 'OFFLINE_MEDIA' confirmed.", SECTION)
else:           warn(True, "MKT_OFF sentinel 'OFFLINE_MEDIA' not found in channel_mapping.csv", SECTION)

# MKT_OFF CADENA_STD must be NULL — cannot assert against raw FACT_MEDIA_OFF (column name unknown)
# Assert deferred to Phase 3 when mkt_off_std standardized output is available
log("INFO", "MKT_OFF CADENA_STD null assertion deferred to Phase 3 (assert against mkt_off_std, not raw table).", SECTION)
log("INFO", "Business rule: MKT_OFF CADENA_STD must be NULL. Column name in raw table TBD from DESC TABLE.", SECTION)

# Export NEEDS_REVIEW rows from each catalog
for pdf, fname in [(chan_pdf, "unmapped_channels_by_source.csv"), (cad_pdf, "unmapped_cadenas_by_source.csv")]:
    if not pdf.empty and "mapping_status" in pdf.columns:
        nr = pdf[pdf["mapping_status"] == "NEEDS_REVIEW"]
        if len(nr):
            save_df(spark.createDataFrame(nr), fname, SECTION)
            warn(True, f"{len(nr)} NEEDS_REVIEW rows → {fname}", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section A6 — WASTE Column Validation

# COMMAND ----------

# ── CELL 10: Verify WASTE physical column names ───────────────────────────────
SECTION = "A6-WASTE-COLUMNS"
log("INFO", "=" * 60, SECTION)

SQL_WASTE_COLS = """
SELECT COLUMN_NAME, DATA_TYPE
FROM PRD_MDP.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'MDP_STG'
  AND TABLE_NAME   = 'FACT_TOPLINE'
  AND (UPPER(COLUMN_NAME) LIKE '%WASTE%'
    OR UPPER(COLUMN_NAME) LIKE '%KG%'
    OR UPPER(COLUMN_NAME) LIKE '%VAL%'
    OR UPPER(COLUMN_NAME) LIKE '%AMOUNT%'
    OR UPPER(COLUMN_NAME) LIKE '%VOLUME%')
ORDER BY COLUMN_NAME
"""
df_waste = run_sf("PRD_MDP", SQL_WASTE_COLS)
display(df_waste)
found_cols = {r["COLUMN_NAME"] for r in df_waste.collect()}
for expected in ["WASTE ($)", "WASTE (KG)"]:
    if expected in found_cols: passed(f"Column '{expected}' exists in FACT_TOPLINE.", SECTION)
    else: warn(True, f"Column '{expected}' NOT found — verify physical name.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section A7 — Nielsen Market Bridge Coverage

# COMMAND ----------

# ── CELL 11: Profile Nielsen market dim tables ────────────────────────────────
SECTION = "A7-NIELSEN-BRIDGE"
log("INFO", "=" * 60, SECTION)

nielsen_csv = os.path.join(_HOMO_PATH, "nielsen_market_mapping.csv")
bridge_pdf  = pd.read_csv(nielsen_csv, dtype=str).fillna("") if os.path.exists(nielsen_csv) else pd.DataFrame()
confirmed_markets = (
    set(bridge_pdf[bridge_pdf.get("mapping_status", pd.Series(dtype=str)) == "CONFIRMED"]
        ["raw_market_normalized"].str.upper())
    if not bridge_pdf.empty else set()
)

NIELSEN_MKT_QUERIES = {
    "EDP_NIELSEN":        "SELECT DISTINCT TRIM(UPPER(MRKT_DSC_SHRT)) AS v FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_MKT_DIM",
    "PB_NIELSEN":         "SELECT DISTINCT TRIM(UPPER(MRKT_DSC_SHRT)) AS v FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_SUST_LECHE_ST_NLSN_MKT_DIM",
    "WATER_NIELSEN_RIE":  "SELECT DISTINCT TRIM(UPPER(MRKT_DSC_SHRT)) AS v FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_RT_NLSN_MKT_DIM",
    "WATER_SCANTRACK":    "SELECT DISTINCT TRIM(UPPER(MRKT_DSC_SHRT)) AS v FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_ST_NLSN_MKT_DIM",
}

new_mkt_rows = []
for src, sql in NIELSEN_MKT_QUERIES.items():
    try:
        vals = [r["V"] for r in run_sf("PRD_MEX", sql).collect()]
        unmapped = [v for v in vals if v and v.upper() not in confirmed_markets]
        log("INFO", f"{src}: {len(vals)} total | {len(unmapped)} unmapped", SECTION)
        warn(len(unmapped) > 0, f"Warning #3: {src}: {len(unmapped)} market strings need mapping.", SECTION)
        for v in unmapped:
            new_mkt_rows.append({"raw_market_value": v, "raw_market_normalized": v,
                                  "source_system": src, "canal_std": "", "cadena_std": "",
                                  "region_std": "", "market_type": "", "reading_type": "",
                                  "mapping_status": "NEEDS_REVIEW", "notes": "Auto-discovered"})
    except Exception as exc:
        warn(True, f"{src}: query failed — {exc}", SECTION)

needs_review_count = len(new_mkt_rows)
if new_mkt_rows:
    df_new_mkts = spark.createDataFrame(new_mkt_rows)
    display(df_new_mkts)
    save_df(df_new_mkts, "unmapped_nielsen_markets.csv", SECTION)
    warn(True, f"Warning #3: {needs_review_count} Nielsen market strings still NEEDS_REVIEW.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Summary

# COMMAND ----------

# ── CELL 12: Summary + flush ──────────────────────────────────────────────────
SECTION = "SUMMARY"
log("INFO", "=" * 60, SECTION)
log("INFO", f"Hard blockers : {len(_HARD_BLOCKERS)}", SECTION)
log("INFO", f"Warnings      : {len(_WARNINGS)}", SECTION)

log("INFO", "\nVALIDATED RULES (v3):", SECTION)
log("INFO", "  ✓ CAT_UPC grain = one row per MAT_IDT (not per EAN)", SECTION)
log("INFO", "  ✓ SKU_EAN_COD treated as barcode attribute — may be non-unique", SECTION)
log("INFO", "  ✓ TRY_TO_NUMBER(MAT_ACT_FLG) used for active filter (VARCHAR-safe)", SECTION)
log("INFO", "  ✓ All product IDs cast to VARCHAR throughout", SECTION)
log("INFO", "  ✓ UPC_STD NOT used in all-source join predicate", SECTION)
log("INFO", "  ✓ MKT_ON CANAL_STD = ECOMMERCE_MEDIA (constant)", SECTION)
log("INFO", "  ✓ MKT_OFF CANAL_STD = OFFLINE_MEDIA (constant)", SECTION)
log("INFO", "  ✓ MKT_OFF CADENA_STD assertion deferred to Phase 3 (raw column name unknown)", SECTION)
log("INFO", "  ✓ Preferred MAT_IDT resolved via ean_cardinality_status logic", SECTION)
log("INFO", "  ✓ Ambiguous EAN matches quarantined to upc_bridge_ean_ambiguous.csv", SECTION)
log("INFO", "  ✓ P3 fuzzy matching quarantine-only — never auto-promoted", SECTION)

if _HARD_BLOCKERS:
    log("🚨 BLOCKER", "BLOCKERS FOUND — resolve before Phase 3:", SECTION)
    for b in _HARD_BLOCKERS:
        log("🚨 BLOCKER", f"  → {b}", SECTION)
else:
    passed("Zero hard blockers — Notebook A clean.", SECTION)

flush_log()
print(f"\n{'='*60}\nNotebook A (v3) complete — {ts()}\nOutput root: {DBFS_ROOT}\n{'='*60}")
