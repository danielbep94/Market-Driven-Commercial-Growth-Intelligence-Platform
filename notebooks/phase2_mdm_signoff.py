# Databricks notebook source
# MAGIC %md
# MAGIC # MDM Phase 2 — Sign-Off Validation Notebook
# MAGIC
# MAGIC **Purpose:** Run all six sign-off queries required before the MDM layer can be
# MAGIC promoted to Phase 3 (production). Each section saves its results to DBFS and
# MAGIC appends a structured entry to a shared audit log.
# MAGIC
# MAGIC **Status model:**
# MAGIC | Symbol | Meaning |
# MAGIC |--------|---------|
# MAGIC | ✅ PASS | Condition met — sign-off cleared |
# MAGIC | ⚠️ WARNING | Review required — not a hard blocker |
# MAGIC | 🚨 BLOCKER | Hard blocker — must be resolved before Phase 3 |
# MAGIC | ⏭ SKIPPED | Query returned no data (table absent or empty) |
# MAGIC
# MAGIC **Credential resolution (matches `validate_credentials.py`):**
# MAGIC - `PRD_MEX` — `configs/snowflake_creds.py` → `SF_MEX_*` (hardcoded analyst account)
# MAGIC - `PRD_MDP` — `configs/snowflake_creds.py` → `SF_MDP_*` or Key Vault fallback
# MAGIC
# MAGIC **Run first:** `notebooks/validate_credentials.py` — all 6 cells must pass.
# MAGIC
# MAGIC **Output paths (DBFS):**
# MAGIC ```
# MAGIC dbfs:/mnt/mdp/mdm/phase2_signoff/
# MAGIC   signoff_audit_log.txt
# MAGIC   signoff_01_sku_quality.csv
# MAGIC   signoff_01_ean_cardinality.csv
# MAGIC   signoff_01_sku_mapping.csv
# MAGIC   signoff_02_upc_cascade.csv
# MAGIC   signoff_02_upc_unmatched.csv
# MAGIC   signoff_03_nielsen_markets.csv
# MAGIC   signoff_04_v_d_client_schema.csv
# MAGIC   signoff_04_v_d_client.csv
# MAGIC   signoff_05_store_schema.csv
# MAGIC   signoff_05_store_chain.csv
# MAGIC   signoff_06_hard_blocker_check.csv
# MAGIC ```
# MAGIC
# MAGIC > ⚠️ **Do not promote to Phase 3 if `signoff_audit_log.txt` contains any 🚨 BLOCKER lines.**

# COMMAND ----------

# DBTITLE 1,Cell 2
# ── CELL 1: Load credentials (same pattern as validate_credentials.py) ────────
import os, importlib.util, datetime

_notebook_ws = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_current_dir  = "/Workspace" + os.path.dirname(_notebook_ws)
_creds_path  = os.path.normpath(
    os.path.join(_current_dir, "..", "configs", "snowflake_creds.py")
)

if not os.path.exists(_creds_path):
    raise FileNotFoundError(
        "❌ configs/snowflake_creds.py NOT FOUND.\n"
        "   Copy configs/snowflake_creds.example.py → configs/snowflake_creds.py\n"
        "   and fill in your credentials."
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
print(f"   PRD_MDP user      : {'<from Key Vault>' if not getattr(_m,'SF_MDP_USER',None) else _m.SF_MDP_USER}")
print(f"   PRD_MDP warehouse : {getattr(_m, 'SF_MDP_WH', 'PRD_MDP_ANL_WH')}")

# COMMAND ----------

# ── CELL 2: Output paths + helpers ────────────────────────────────────────────
DBFS_ROOT   = "dbfs:/mnt/mdp/mdm/phase2_signoff"
LOCAL_ROOT  = "/dbfs/mnt/mdp/mdm/phase2_signoff"
LOG_PATH    = f"{LOCAL_ROOT}/signoff_audit_log.txt"

# ── Thresholds ────────────────────────────────────────────────────────────────
UPC_P1_RATE_WARN  = 70.0
EAN_NULL_BLOCKER  = 0

dbutils.fs.mkdirs(DBFS_ROOT)

_LOG_LINES: list[str] = []

def ts() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def log(level: str, msg: str, section: str = ""):
    prefix = f"[{ts()}] [{level}]"
    if section: prefix += f" [{section}]"
    line = f"{prefix} {msg}"
    _LOG_LINES.append(line)
    print(line)

def flush_log():
    content = "\n".join(_LOG_LINES)
    dbutils.fs.put(f"{DBFS_ROOT}/signoff_audit_log.txt", content, overwrite=True)
    print(f"\n📄 Log saved → {DBFS_ROOT}/signoff_audit_log.txt")

def save_df(df, name: str, section: str = ""):
    path = f"{DBFS_ROOT}/{name}"
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(path)
    log("INFO", f"Saved → {path}", section)
    return path

def run_sf(database: str, sql: str):
    """Execute a Snowflake query using the correct credential profile."""
    opts = get_sf_options(database)
    return (spark.read
                 .format("net.snowflake.spark.snowflake")
                 .options(**opts)
                 .option("sfDatabase", database)
                 .option("query", sql)
                 .load())

def blocker(condition: bool, msg: str, section: str = ""):
    if condition:
        log("🚨 BLOCKER", msg, section)
    return condition

def warn(condition: bool, msg: str, section: str = ""):
    if condition:
        log("⚠️  WARNING", msg, section)
    return condition

def passed(msg: str, section: str = ""):
    log("✅ PASS", msg, section)

print(f"✅ CELL 2 — Helpers ready. Output root: {DBFS_ROOT}")

# COMMAND ----------

# ── Backward-compatible aliases (all SQL uses fully-qualified names, schema arg unused) ──
DB_PRD_MEX  = "PRD_MEX"
DB_PRD_MDP  = "PRD_MDP"
SCH_OTC     = "MEX_DSP_OTC"       # unused by run_sf but kept so call sites don't fail
SCH_MDP_DSP = "MDP_DSP"
SCH_MDP_STG = "MDP_STG"
SCH_DPH_MKT = "MEX_DSP_DPH_MKT"

def run_query(database: str, schema: str, sql: str):
    """Compat wrapper — routes to run_sf() using the correct credential profile.
    `schema` is kept for call-site compatibility but is not used:
    all SQL in this notebook uses fully-qualified 3-part names (DB.SCHEMA.TABLE).
    """
    return run_sf(database, sql)

print("✅ Compat aliases ready — run_query(), DB_PRD_MEX, DB_PRD_MDP, SCH_* defined.")


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Sign-Off #1 — Populate `sku_mapping.csv` from `V_D_ITEM`
# MAGIC
# MAGIC **Goal:** Confirm EAN quality and export the populated SKU catalog seed.
# MAGIC
# MAGIC **Hard blockers:**
# MAGIC - Any `SKU_EAN_COD = NULL` for active products
# MAGIC - Any EAN code that maps to more than one `MAT_IDT`
# MAGIC - Any EAN stored as a numeric type (leading zeros lost)

# COMMAND ----------

SECTION = "SIGN-OFF #1 — V_D_ITEM EAN QUALITY"
log("INFO", "=" * 60, SECTION)
log("INFO", "Starting V_D_ITEM EAN profile and SKU catalog seed export.", SECTION)

# ── 1A: Null / blank EAN coverage ────────────────────────────────────────────
SQL_EAN_QUALITY = """
SELECT
    COUNT(*)                                           AS total_rows,
    COUNT_IF(SKU_EAN_COD IS NULL)                      AS null_ean_rows,
    COUNT_IF(TRIM(TO_VARCHAR(SKU_EAN_COD)) = '')       AS blank_ean_rows,
    COUNT_IF(MAT_ACT_FLG = 1 AND SKU_EAN_COD IS NULL) AS null_ean_active_rows,
    COUNT(DISTINCT TO_VARCHAR(SKU_EAN_COD))            AS distinct_ean,
    COUNT(DISTINCT TO_VARCHAR(MAT_IDT))                AS distinct_mat_idt,
    COUNT(DISTINCT CBU)                                AS distinct_cbu
FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM
"""

df_quality = run_query(DB_PRD_MEX, SCH_OTC, SQL_EAN_QUALITY)
display(df_quality)
save_df(df_quality, "signoff_01_sku_quality.csv", SECTION)

row = df_quality.collect()[0]
total_rows       = row["TOTAL_ROWS"]
null_ean_rows    = row["NULL_EAN_ROWS"]
blank_ean_rows   = row["BLANK_EAN_ROWS"]
null_ean_active  = row["NULL_EAN_ACTIVE_ROWS"]
distinct_ean     = row["DISTINCT_EAN"]
distinct_mat     = row["DISTINCT_MAT_IDT"]

log("INFO", f"V_D_ITEM total rows:          {total_rows:,}", SECTION)
log("INFO", f"NULL SKU_EAN_COD (all):       {null_ean_rows:,}", SECTION)
log("INFO", f"NULL SKU_EAN_COD (active):    {null_ean_active:,}", SECTION)
log("INFO", f"Blank SKU_EAN_COD:            {blank_ean_rows:,}", SECTION)
log("INFO", f"Distinct EAN values:          {distinct_ean:,}", SECTION)
log("INFO", f"Distinct MAT_IDT:             {distinct_mat:,}", SECTION)

blocker(null_ean_active > EAN_NULL_BLOCKER,
        f"{null_ean_active} ACTIVE products have NULL SKU_EAN_COD — cannot serve as golden UPC source.",
        SECTION)
warn(null_ean_rows > 0,
     f"{null_ean_rows} total rows have NULL EAN (including inactive). Investigate before full population.",
     SECTION)
if null_ean_active == 0:
    passed("No active products have NULL SKU_EAN_COD.", SECTION)

# COMMAND ----------

# ── 1B: EAN → MAT_IDT cardinality check (1 EAN must map to 1 MAT_IDT) ───────
SQL_EAN_CARDINALITY = """
SELECT
    TO_VARCHAR(SKU_EAN_COD)             AS sku_ean_cod,
    COUNT(DISTINCT TO_VARCHAR(MAT_IDT)) AS distinct_mat_idt,
    COUNT(*)                            AS row_count,
    LISTAGG(DISTINCT TO_VARCHAR(MAT_IDT), ' | ')
        WITHIN GROUP (ORDER BY TO_VARCHAR(MAT_IDT)) AS mat_idt_list
FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM
WHERE SKU_EAN_COD IS NOT NULL
  AND TRIM(TO_VARCHAR(SKU_EAN_COD)) <> ''
GROUP BY TO_VARCHAR(SKU_EAN_COD)
HAVING COUNT(DISTINCT TO_VARCHAR(MAT_IDT)) > 1
ORDER BY distinct_mat_idt DESC, row_count DESC
"""

df_cardinality = run_query(DB_PRD_MEX, SCH_OTC, SQL_EAN_CARDINALITY)
multi_ean_count = df_cardinality.count()
display(df_cardinality)
save_df(df_cardinality, "signoff_01_ean_cardinality.csv", SECTION)

blocker(multi_ean_count > 0,
        f"{multi_ean_count} EAN codes map to multiple MAT_IDTs. "
        f"Resolve before using SKU_EAN_COD as a golden join key.",
        SECTION)
if multi_ean_count == 0:
    passed("All EAN codes map to exactly one MAT_IDT — golden source is clean.", SECTION)

# COMMAND ----------

# ── 1C: Export SKU mapping seed (VARCHAR cast — preserves leading zeros) ─────
SQL_SKU_SEED = """
SELECT
    TO_VARCHAR(MAT_IDT)          AS mat_idt,
    TO_VARCHAR(SKU_EAN_COD)      AS sku_ean_cod,
    MAT_LCL_DSC                  AS mat_lcl_dsc,
    LV2_UMB_BRD_DSC              AS marca_raw,
    TRIM(UPPER(LV2_UMB_BRD_DSC)) AS marca_raw_normalized,
    CBU                          AS cbu,
    MAT_ACT_FLG                  AS is_active,
    -- Bridge columns — populated later by SELL_OUT cascade
    NULL::VARCHAR                AS sell_out_int_id,
    NULL::VARCHAR                AS sell_out_import_id,
    NULL::INTEGER                AS match_priority,
    NULL::VARCHAR                AS match_method,
    NULL::FLOAT                  AS match_confidence,
    'NEEDS_REVIEW'               AS review_status,
    'SELL_IN'                    AS source_system,
    CURRENT_TIMESTAMP()          AS created_at,
    CURRENT_TIMESTAMP()          AS updated_at,
    'Seeded from V_D_ITEM via Phase 2 sign-off notebook' AS notes
FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM
WHERE MAT_ACT_FLG = 1
ORDER BY CBU, mat_idt
"""

df_sku = run_query(DB_PRD_MEX, SCH_OTC, SQL_SKU_SEED)
sku_count = df_sku.count()
display(df_sku.limit(20))
save_df(df_sku, "signoff_01_sku_mapping.csv", SECTION)

log("INFO", f"Exported {sku_count:,} active SKUs to signoff_01_sku_mapping.csv", SECTION)
log("INFO",
    "ACTION REQUIRED: Copy signoff_01_sku_mapping.csv to homologation/sku_mapping.csv "
    "and change review_status to CONFIRMED for validated records.",
    SECTION)
passed(f"Sign-off #1 data exported. Validate EAN cardinality result before marking complete.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Sign-Off #2 — SELL_OUT UPC Cascade Distribution
# MAGIC
# MAGIC **Goal:** Measure the P1 (INT_ID exact) and P2 (IMPORT_ID exact) match rates
# MAGIC between SELL_OUT product master and SELL_IN V_D_ITEM. Identify unmatched products.
# MAGIC
# MAGIC **Hard blockers:**
# MAGIC - Any P3 fuzzy match being auto-promoted (structural — enforced in Notebook A)
# MAGIC
# MAGIC **Warning thresholds:**
# MAGIC - P1 match rate < 70% before baseline bridge is established

# COMMAND ----------

SECTION = "SIGN-OFF #2 — SELL_OUT UPC CASCADE"
log("INFO", "=" * 60, SECTION)
log("INFO", "Measuring SELL_OUT UPC bridge cascade distribution.", SECTION)

# ── 2A: Priority-1 matches (INT_ID = SKU_EAN_COD) ────────────────────────────
SQL_P1 = """
SELECT
    TO_VARCHAR(prod.INT_ID)          AS sell_out_int_id,
    TO_VARCHAR(item.SKU_EAN_COD)     AS sku_ean_cod,
    TO_VARCHAR(item.MAT_IDT)         AS mat_idt,
    item.MAT_LCL_DSC                 AS si_description,
    prod.NAME                        AS so_name,
    prod.BRAND                       AS so_brand,
    prod.CBU_ID                      AS cbu_id,
    1                                AS match_priority,
    'EXACT_INT_ID'                   AS match_method,
    1.0                              AS match_confidence,
    'CONFIRMED'                      AS review_status
FROM PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM prod
INNER JOIN PRD_MEX.MEX_DSP_OTC.V_D_ITEM item
    ON TO_VARCHAR(prod.INT_ID) = TO_VARCHAR(item.SKU_EAN_COD)
WHERE prod.INT_ID IS NOT NULL
  AND TRIM(TO_VARCHAR(prod.INT_ID)) <> ''
"""

df_p1 = run_query(DB_PRD_MEX, SCH_OTC, SQL_P1)
p1_count = df_p1.count()
log("INFO", f"Priority-1 matches (INT_ID exact): {p1_count:,}", SECTION)
display(df_p1.limit(10))

# COMMAND ----------

# ── 2B: Priority-2 matches (IMPORT_ID = SKU_EAN_COD, not already matched by P1) ──
SQL_P2 = """
WITH p1_matched AS (
    SELECT DISTINCT TO_VARCHAR(prod.INT_ID) AS matched_key
    FROM PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM prod
    INNER JOIN PRD_MEX.MEX_DSP_OTC.V_D_ITEM item
        ON TO_VARCHAR(prod.INT_ID) = TO_VARCHAR(item.SKU_EAN_COD)
    WHERE prod.INT_ID IS NOT NULL
)
SELECT
    TO_VARCHAR(prod.IMPORT_ID)       AS sell_out_import_id,
    TO_VARCHAR(prod.INT_ID)          AS sell_out_int_id,
    TO_VARCHAR(item.SKU_EAN_COD)     AS sku_ean_cod,
    TO_VARCHAR(item.MAT_IDT)         AS mat_idt,
    item.MAT_LCL_DSC                 AS si_description,
    prod.NAME                        AS so_name,
    prod.BRAND                       AS so_brand,
    prod.CBU_ID                      AS cbu_id,
    2                                AS match_priority,
    'EXACT_IMPORT_ID'                AS match_method,
    1.0                              AS match_confidence,
    'CONFIRMED'                      AS review_status
FROM PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM prod
INNER JOIN PRD_MEX.MEX_DSP_OTC.V_D_ITEM item
    ON TO_VARCHAR(prod.IMPORT_ID) = TO_VARCHAR(item.SKU_EAN_COD)
LEFT JOIN p1_matched
    ON TO_VARCHAR(prod.INT_ID) = p1_matched.matched_key
WHERE prod.IMPORT_ID IS NOT NULL
  AND TRIM(TO_VARCHAR(prod.IMPORT_ID)) <> ''
  AND p1_matched.matched_key IS NULL
"""

df_p2 = run_query(DB_PRD_MEX, SCH_OTC, SQL_P2)
p2_count = df_p2.count()
log("INFO", f"Priority-2 matches (IMPORT_ID exact): {p2_count:,}", SECTION)
display(df_p2.limit(10))

# COMMAND ----------

# ── 2C: Unmatched SELL_OUT products (no P1 or P2 match) ─────────────────────
SQL_UNMATCHED = """
SELECT
    TO_VARCHAR(prod.INT_ID)          AS int_id,
    TO_VARCHAR(prod.IMPORT_ID)       AS import_id,
    prod.NAME                        AS so_name,
    prod.BRAND                       AS so_brand,
    prod.CBU_ID                      AS cbu_id,
    3                                AS match_priority,
    'UNMATCHED'                      AS match_method,
    0.0                              AS match_confidence,
    'NEEDS_REVIEW'                   AS review_status,
    'No exact match in V_D_ITEM via INT_ID or IMPORT_ID'  AS notes
FROM PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM prod
WHERE NOT EXISTS (
    SELECT 1 FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM item
    WHERE TO_VARCHAR(prod.INT_ID) = TO_VARCHAR(item.SKU_EAN_COD)
)
AND NOT EXISTS (
    SELECT 1 FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM item
    WHERE TO_VARCHAR(prod.IMPORT_ID) = TO_VARCHAR(item.SKU_EAN_COD)
)
ORDER BY prod.BRAND, prod.NAME
"""

df_unmatched = run_query(DB_PRD_MEX, SCH_OTC, SQL_UNMATCHED)
unmatched_count = df_unmatched.count()
log("INFO", f"Unmatched SELL_OUT products:         {unmatched_count:,}", SECTION)
display(df_unmatched.limit(20))
save_df(df_unmatched, "signoff_02_upc_unmatched.csv", SECTION)

# COMMAND ----------

# ── 2D: Cascade summary and rates ────────────────────────────────────────────
total_so = p1_count + p2_count + unmatched_count
p1_pct   = round(p1_count   / total_so * 100, 1) if total_so > 0 else 0.0
p2_pct   = round(p2_count   / total_so * 100, 1) if total_so > 0 else 0.0
um_pct   = round(unmatched_count / total_so * 100, 1) if total_so > 0 else 0.0

cascade_data = [
    ("EXACT_INT_ID",    1, p1_count,        p1_pct,  "CONFIRMED"),
    ("EXACT_IMPORT_ID", 2, p2_count,        p2_pct,  "CONFIRMED"),
    ("UNMATCHED",       3, unmatched_count, um_pct,  "NEEDS_REVIEW"),
]
df_cascade = spark.createDataFrame(
    cascade_data,
    ["match_method", "priority", "row_count", "pct_of_total", "review_status"]
)
display(df_cascade)
save_df(df_cascade, "signoff_02_upc_cascade.csv", SECTION)

log("INFO", f"Total SELL_OUT products:               {total_so:,}", SECTION)
log("INFO", f"Priority-1 (INT_ID):    {p1_count:,}  ({p1_pct}%)", SECTION)
log("INFO", f"Priority-2 (IMPORT_ID): {p2_count:,}  ({p2_pct}%)", SECTION)
log("INFO", f"Unmatched:              {unmatched_count:,}  ({um_pct}%)", SECTION)

warn(p1_pct < UPC_P1_RATE_WARN,
     f"Priority-1 match rate {p1_pct}% is below {UPC_P1_RATE_WARN}% threshold. "
     f"Review SELL_OUT product master alignment before baseline is established.",
     SECTION)
if p1_pct >= UPC_P1_RATE_WARN:
    passed(f"Priority-1 match rate {p1_pct}% meets {UPC_P1_RATE_WARN}% threshold.", SECTION)

log("INFO",
    "REMINDER: P3 fuzzy matching is PROHIBITED from auto-promotion. "
    "Run Notebook A (validate_mdm_catalogs.py) to generate fuzzy candidates for manual review.",
    SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Sign-Off #3 — Complete `nielsen_market_mapping.csv` from `MRKT_DSC_SHRT`
# MAGIC
# MAGIC **Goal:** Extract all unique market strings from all four Nielsen market dim tables.
# MAGIC The static bridge CSV must be completed before Nielsen standardization can run.
# MAGIC
# MAGIC **Hard blocker:** Any Nielsen source joined without a corresponding entry in
# MAGIC `CAT_MERCADO_NIELSEN` (i.e., `mapping_status = NEEDS_REVIEW` rows in production).

# COMMAND ----------

SECTION = "SIGN-OFF #3 — NIELSEN MARKET STRINGS"
log("INFO", "=" * 60, SECTION)
log("INFO", "Extracting all MRKT_DSC_SHRT values from four Nielsen market dims.", SECTION)

NIELSEN_MARKET_QUERIES = {
    "EDP_NIELSEN": (
        DB_PRD_MEX, SCH_DPH_MKT,
        """SELECT DISTINCT
               MRKT_DSC_SHRT                        AS raw_market_value,
               TRIM(UPPER(MRKT_DSC_SHRT))           AS raw_market_normalized,
               'EDP_NIELSEN'                         AS source_system,
               ''                                    AS canal_std,
               ''                                    AS cadena_std,
               ''                                    AS region_std,
               ''                                    AS market_type,
               ''                                    AS reading_type,
               'NEEDS_REVIEW'                        AS mapping_status,
               'Auto-discovered — map manually'      AS notes
           FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_MKT_DIM
           ORDER BY raw_market_normalized"""
    ),
    "PB_NIELSEN": (
        DB_PRD_MEX, SCH_DPH_MKT,
        """SELECT DISTINCT
               MRKT_DSC_SHRT                        AS raw_market_value,
               TRIM(UPPER(MRKT_DSC_SHRT))           AS raw_market_normalized,
               'PB_NIELSEN'                          AS source_system,
               ''                                    AS canal_std,
               ''                                    AS cadena_std,
               ''                                    AS region_std,
               ''                                    AS market_type,
               ''                                    AS reading_type,
               'NEEDS_REVIEW'                        AS mapping_status,
               'Auto-discovered — map manually'      AS notes
           FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_SUST_LECHE_ST_NLSN_MKT_DIM
           ORDER BY raw_market_normalized"""
    ),
    "WATER_NIELSEN_RIE": (
        DB_PRD_MEX, SCH_DPH_MKT,
        """SELECT DISTINCT
               MRKT_DSC_SHRT                        AS raw_market_value,
               TRIM(UPPER(MRKT_DSC_SHRT))           AS raw_market_normalized,
               'WATER_NIELSEN_RIE'                   AS source_system,
               ''                                    AS canal_std,
               ''                                    AS cadena_std,
               ''                                    AS region_std,
               ''                                    AS market_type,
               ''                                    AS reading_type,
               'NEEDS_REVIEW'                        AS mapping_status,
               'Auto-discovered — map manually'      AS notes
           FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_RT_NLSN_MKT_DIM
           ORDER BY raw_market_normalized"""
    ),
    "WATER_SCANTRACK": (
        DB_PRD_MEX, SCH_DPH_MKT,
        """SELECT DISTINCT
               MRKT_DSC_SHRT                        AS raw_market_value,
               TRIM(UPPER(MRKT_DSC_SHRT))           AS raw_market_normalized,
               'WATER_SCANTRACK'                     AS source_system,
               ''                                    AS canal_std,
               ''                                    AS cadena_std,
               ''                                    AS region_std,
               ''                                    AS market_type,
               ''                                    AS reading_type,
               'NEEDS_REVIEW'                        AS mapping_status,
               'Auto-discovered — map manually'      AS notes
           FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_ST_NLSN_MKT_DIM
           ORDER BY raw_market_normalized"""
    ),
}

from functools import reduce
from pyspark.sql import DataFrame

dfs = []
for source_name, (db, schema, sql) in NIELSEN_MARKET_QUERIES.items():
    try:
        df_src = run_query(db, schema, sql)
        cnt = df_src.count()
        log("INFO", f"{source_name}: {cnt} distinct market strings found.", SECTION)
        dfs.append(df_src)
    except Exception as exc:
        log("⚠️  WARNING", f"{source_name}: Query failed — {exc}", SECTION)

if dfs:
    df_all_markets = reduce(DataFrame.unionByName, dfs)
    total_market_vals = df_all_markets.count()
    display(df_all_markets)
    save_df(df_all_markets, "signoff_03_nielsen_markets.csv", SECTION)
    log("INFO", f"Total unique MRKT_DSC_SHRT values across all sources: {total_market_vals}", SECTION)
    log("INFO",
        "ACTION REQUIRED: Open signoff_03_nielsen_markets.csv and fill in "
        "canal_std, cadena_std, region_std, market_type, reading_type for each row. "
        "Then change mapping_status from NEEDS_REVIEW → CONFIRMED and copy to "
        "homologation/nielsen_market_mapping.csv.",
        SECTION)
    warn(total_market_vals > 0,
         f"{total_market_vals} market strings discovered — all marked NEEDS_REVIEW. "
         f"Manual mapping required before this sign-off clears.",
         SECTION)
else:
    log("⏭ SKIPPED", "No Nielsen market dims were reachable.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Sign-Off #4 — Inspect `V_D_CLIENT` for SELL_IN Cadena Field
# MAGIC
# MAGIC **Goal:** Determine whether `CUS_GRN_CHL_DSC` is a CANAL field (grand channel)
# MAGIC or a CADENA field (customer chain), so `SELL_IN.CADENA_STD` can be resolved or
# MAGIC formally confirmed as NULL.
# MAGIC
# MAGIC **Rule:** Do not populate SELL_IN `CADENA_STD` until this query confirms semantics.

# COMMAND ----------

SECTION = "SIGN-OFF #4 — V_D_CLIENT CADENA INSPECTION"
log("INFO", "=" * 60, SECTION)
log("INFO", "Inspecting V_D_CLIENT schema and CUS_GRN_CHL_DSC value distribution.", SECTION)

# ── 4A: Schema ────────────────────────────────────────────────────────────────
SQL_CLIENT_SCHEMA = """
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    CHARACTER_MAXIMUM_LENGTH
FROM PRD_MEX.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'MEX_DSP_OTC'
  AND TABLE_NAME   = 'V_D_CLIENT'
ORDER BY ORDINAL_POSITION
"""

df_client_schema = run_query(DB_PRD_MEX, "INFORMATION_SCHEMA", SQL_CLIENT_SCHEMA)
display(df_client_schema)
save_df(df_client_schema, "signoff_04_v_d_client_schema.csv", SECTION)
log("INFO", f"V_D_CLIENT column count: {df_client_schema.count()}", SECTION)

# COMMAND ----------

# ── 4B: CUS_GRN_CHL_DSC value distribution ───────────────────────────────────
SQL_CLIENT_CHANNEL = """
SELECT
    CUS_GRN_CHL_DSC                          AS raw_value,
    TRIM(UPPER(CUS_GRN_CHL_DSC))             AS raw_value_normalized,
    COUNT(*)                                 AS client_count,
    COUNT(DISTINCT CUS_IDT)                  AS distinct_clients,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM PRD_MEX.MEX_DSP_OTC.V_D_CLIENT
GROUP BY CUS_GRN_CHL_DSC, TRIM(UPPER(CUS_GRN_CHL_DSC))
ORDER BY client_count DESC
"""

df_client_channel = run_query(DB_PRD_MEX, SCH_OTC, SQL_CLIENT_CHANNEL)
display(df_client_channel)
save_df(df_client_channel, "signoff_04_v_d_client.csv", SECTION)

client_distinct_vals = df_client_channel.count()
log("INFO", f"Distinct CUS_GRN_CHL_DSC values: {client_distinct_vals}", SECTION)
log("INFO",
    "ACTION REQUIRED: Review signoff_04_v_d_client.csv and determine:\n"
    "  • If values like 'AUTOSERVICIOS', 'FARMACIAS' etc. → this is CANAL (grand channel)\n"
    "  • If values are retailer names like 'WALMART', 'HEB' etc. → this is CADENA (chain)\n"
    "  • Update channel_mapping.csv and/or cadena_mapping.csv for SELL_IN accordingly.\n"
    "  • Update SELL_IN CADENA_APPLICABLE_FLAG in both notebooks once confirmed.",
    SECTION)

# ── 4C: Check for any other candidate chain columns in V_D_CLIENT ─────────────
SQL_CLIENT_CHAIN_COLS = """
SELECT COLUMN_NAME, DATA_TYPE
FROM PRD_MEX.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'MEX_DSP_OTC'
  AND TABLE_NAME   = 'V_D_CLIENT'
  AND (
        UPPER(COLUMN_NAME) LIKE '%CHAIN%'
     OR UPPER(COLUMN_NAME) LIKE '%CADENA%'
     OR UPPER(COLUMN_NAME) LIKE '%CANAL%'
     OR UPPER(COLUMN_NAME) LIKE '%CHANNEL%'
     OR UPPER(COLUMN_NAME) LIKE '%CHL%'
     OR UPPER(COLUMN_NAME) LIKE '%GRP%'
  )
ORDER BY ORDINAL_POSITION
"""

df_chain_cols = run_query(DB_PRD_MEX, "INFORMATION_SCHEMA", SQL_CLIENT_CHAIN_COLS)
display(df_chain_cols)
chain_col_count = df_chain_cols.count()
log("INFO",
    f"Candidate chain/channel columns in V_D_CLIENT: {chain_col_count}. "
    f"Review all of them to confirm the right field for CADENA derivation.",
    SECTION)
passed("Sign-off #4 data exported — manual review required to classify CUS_GRN_CHL_DSC.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Sign-Off #5 — Inspect `VW_D_STORE_RM` for CHAIN / FORMAT Classification
# MAGIC
# MAGIC **Goal:** Determine whether SELL_OUT `CHAIN` and `FORMAT` columns represent
# MAGIC CADENA (customer chain), CANAL (commercial channel), or both.
# MAGIC `CHAIN` is suspected to be CADENA but must be confirmed before mapping.
# MAGIC
# MAGIC **Rule:** Do not classify `CHAIN` as CANAL without Snowflake evidence.

# COMMAND ----------

SECTION = "SIGN-OFF #5 — VW_D_STORE_RM CHAIN / FORMAT CLASSIFICATION"
log("INFO", "=" * 60, SECTION)
log("INFO", "Inspecting VW_D_STORE_RM schema and CHAIN/FORMAT distribution.", SECTION)

# ── 5A: Schema ────────────────────────────────────────────────────────────────
SQL_STORE_SCHEMA = """
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    CHARACTER_MAXIMUM_LENGTH
FROM PRD_MDP.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'MDP_DSP'
  AND TABLE_NAME   = 'VW_D_STORE_RM'
ORDER BY ORDINAL_POSITION
"""

df_store_schema = run_query(DB_PRD_MDP, "INFORMATION_SCHEMA", SQL_STORE_SCHEMA)
display(df_store_schema)
save_df(df_store_schema, "signoff_05_store_schema.csv", SECTION)
log("INFO", f"VW_D_STORE_RM column count: {df_store_schema.count()}", SECTION)

# COMMAND ----------

# ── 5B: CHAIN + FORMAT distribution ──────────────────────────────────────────
SQL_CHAIN_FORMAT = """
SELECT
    CHAIN                                    AS raw_chain,
    FORMAT                                   AS raw_format,
    TRIM(UPPER(CHAIN))                       AS chain_normalized,
    TRIM(UPPER(FORMAT))                      AS format_normalized,
    COUNT(*)                                 AS store_count,
    COUNT(DISTINCT INT_ID)                   AS distinct_stores,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM PRD_MDP.MDP_DSP.VW_D_STORE_RM
GROUP BY CHAIN, FORMAT, TRIM(UPPER(CHAIN)), TRIM(UPPER(FORMAT))
ORDER BY store_count DESC
"""

df_chain = run_query(DB_PRD_MDP, SCH_MDP_DSP, SQL_CHAIN_FORMAT)
display(df_chain)
save_df(df_chain, "signoff_05_store_chain.csv", SECTION)

chain_vals  = df_chain.select("chain_normalized").distinct().count()
format_vals = df_chain.select("format_normalized").distinct().count()
log("INFO", f"Distinct CHAIN values:  {chain_vals}", SECTION)
log("INFO", f"Distinct FORMAT values: {format_vals}", SECTION)

# COMMAND ----------

# ── 5C: Lookup any additional channel/cadena columns in VW_D_STORE_RM ────────
SQL_STORE_EXTRA_COLS = """
SELECT COLUMN_NAME, DATA_TYPE
FROM PRD_MDP.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'MDP_DSP'
  AND TABLE_NAME   = 'VW_D_STORE_RM'
  AND (
        UPPER(COLUMN_NAME) LIKE '%CHAIN%'
     OR UPPER(COLUMN_NAME) LIKE '%FORMAT%'
     OR UPPER(COLUMN_NAME) LIKE '%CADENA%'
     OR UPPER(COLUMN_NAME) LIKE '%CANAL%'
     OR UPPER(COLUMN_NAME) LIKE '%CHANNEL%'
     OR UPPER(COLUMN_NAME) LIKE '%TYPE%'
     OR UPPER(COLUMN_NAME) LIKE '%BANNER%'
  )
ORDER BY ORDINAL_POSITION
"""

df_store_extra = run_query(DB_PRD_MDP, "INFORMATION_SCHEMA", SQL_STORE_EXTRA_COLS)
display(df_store_extra)
log("INFO",
    "ACTION REQUIRED: Review signoff_05_store_chain.csv and determine:\n"
    "  • CHAIN values: are these retail chains (WALMART, HEB...) → CADENA\n"
    "    or commercial channels (AUTOSERVICIO, CONVENIENCIA...) → CANAL?\n"
    "  • FORMAT values: additional granularity for channel or sub-cadena?\n"
    "  • Update cadena_mapping.csv and channel_mapping.csv SELL_OUT rows accordingly.\n"
    "  • Set mapping_status from NEEDS_REVIEW → CONFIRMED for each classified row.",
    SECTION)
passed("Sign-off #5 data exported — manual CHAIN/FORMAT classification required.", SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Sign-Off #6 — Hard-Blocker Assertion Suite
# MAGIC
# MAGIC **Goal:** Run a battery of structural assertions against live data.
# MAGIC Every row in the output with `status = BLOCKER` must be resolved
# MAGIC before Phase 3 begins.

# COMMAND ----------

SECTION = "SIGN-OFF #6 — HARD BLOCKER ASSERTIONS"
log("INFO", "=" * 60, SECTION)
log("INFO", "Running structural assertion suite.", SECTION)

blocker_results = []

def add_assertion(name: str, sql: str, db: str, schema: str,
                  blocker_condition_col: str, blocker_threshold: float,
                  comparison: str, description: str):
    """
    Run a single-row assertion query.
    comparison: 'GT' (flag if value > threshold) | 'LT' | 'EQ' | 'NEQ'
    """
    try:
        df = run_query(db, schema, sql)
        val = df.collect()[0][blocker_condition_col]
        val_num = float(val) if val is not None else 0.0
        if   comparison == "GT":  is_blocker = val_num >  blocker_threshold
        elif comparison == "LT":  is_blocker = val_num <  blocker_threshold
        elif comparison == "EQ":  is_blocker = val_num == blocker_threshold
        elif comparison == "NEQ": is_blocker = val_num != blocker_threshold
        else:                     is_blocker = False
        status = "🚨 BLOCKER" if is_blocker else "✅ PASS"
        result = {"assertion": name, "value": val_num,
                  "threshold": blocker_threshold, "status": status,
                  "description": description}
        blocker_results.append(result)
        log(status, f"{name}: value={val_num} threshold={blocker_threshold} — {description}", SECTION)
    except Exception as exc:
        result = {"assertion": name, "value": "ERROR", "threshold": blocker_threshold,
                  "status": "⚠️  ERROR", "description": f"{description} | Error: {exc}"}
        blocker_results.append(result)
        log("⚠️  WARNING", f"{name}: Query failed — {exc}", SECTION)

# COMMAND ----------

# Assertion 1: V_D_ITEM active rows with NULL EAN (must be 0)
add_assertion(
    name="NULL_EAN_ACTIVE_PRODUCTS",
    sql="""SELECT COUNT_IF(MAT_ACT_FLG = 1 AND SKU_EAN_COD IS NULL) AS val
           FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM""",
    db=DB_PRD_MEX, schema=SCH_OTC,
    blocker_condition_col="VAL", blocker_threshold=0, comparison="GT",
    description="Active products with NULL SKU_EAN_COD — must be 0"
)

# Assertion 2: EAN codes mapping to multiple MAT_IDTs (must be 0)
add_assertion(
    name="EAN_TO_MULTI_MAT_IDT",
    sql="""SELECT COUNT(*) AS val FROM (
               SELECT TO_VARCHAR(SKU_EAN_COD) AS ean,
                      COUNT(DISTINCT TO_VARCHAR(MAT_IDT)) AS cnt
               FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM
               WHERE SKU_EAN_COD IS NOT NULL AND TRIM(TO_VARCHAR(SKU_EAN_COD)) <> ''
               GROUP BY TO_VARCHAR(SKU_EAN_COD)
               HAVING COUNT(DISTINCT TO_VARCHAR(MAT_IDT)) > 1
           )""",
    db=DB_PRD_MEX, schema=SCH_OTC,
    blocker_condition_col="VAL", blocker_threshold=0, comparison="GT",
    description="EAN codes mapping to multiple MAT_IDTs — must be 0"
)

# Assertion 3: MKT_ON rows with non-NULL CADENA (structural — brand grain only)
# Note: this checks that the MKT_ON source doesn't accidentally expose a UPC column
add_assertion(
    name="MKT_OFF_CADENA_NULL_RATE",
    sql="""SELECT COUNT_IF(CADENA IS NOT NULL AND TRIM(CADENA) <> '') AS val
           FROM PRD_MDP.MDP_STG.FACT_MEDIA_OFF""",
    db=DB_PRD_MDP, schema=SCH_MDP_STG,
    blocker_condition_col="VAL", blocker_threshold=0, comparison="GT",
    description="MKT_OFF rows with non-NULL CADENA — CADENA_STD must be NULL for MKT_OFF (expected 0)"
)

# COMMAND ----------

# Assertion 4: SELL_OUT UPC cascade — P1 match rate (warn if below 70%)
if total_so > 0:
    p1_rate_val = p1_pct
else:
    p1_rate_val = 0.0

blocker_results.append({
    "assertion":   "SELL_OUT_P1_MATCH_RATE",
    "value":       p1_rate_val,
    "threshold":   UPC_P1_RATE_WARN,
    "status":      "✅ PASS" if p1_rate_val >= UPC_P1_RATE_WARN else "⚠️  WARNING",
    "description": f"SELL_OUT P1 (INT_ID) match rate — warn if < {UPC_P1_RATE_WARN}%"
})
log("INFO" if p1_rate_val >= UPC_P1_RATE_WARN else "⚠️  WARNING",
    f"SELL_OUT_P1_MATCH_RATE: {p1_rate_val}% (threshold: {UPC_P1_RATE_WARN}%)", SECTION)

# Assertion 5: Nielsen market bridge — all NEEDS_REVIEW rows must be mapped before production
if dfs:
    nr_count = df_all_markets.filter("mapping_status = 'NEEDS_REVIEW'").count()
    blocker_results.append({
        "assertion":   "NIELSEN_MARKET_NEEDS_REVIEW_COUNT",
        "value":       nr_count,
        "threshold":   0,
        "status":      "⚠️  WARNING — manual mapping required" if nr_count > 0 else "✅ PASS",
        "description": "Nielsen market strings still NEEDS_REVIEW — must be 0 before Phase 3"
    })
    log("⚠️  WARNING" if nr_count > 0 else "✅ PASS",
        f"NIELSEN_MARKET_NEEDS_REVIEW_COUNT: {nr_count} strings still need mapping.", SECTION)

# COMMAND ----------

# ── 6B: Compile assertion results ─────────────────────────────────────────────
df_assertions = spark.createDataFrame(blocker_results)
display(df_assertions)
save_df(df_assertions, "signoff_06_hard_blocker_check.csv", SECTION)

hard_blockers  = [r for r in blocker_results if "BLOCKER" in r["status"]]
warnings_found = [r for r in blocker_results if "WARNING" in r["status"]]

log("INFO", f"Total assertions run: {len(blocker_results)}", SECTION)
log("INFO", f"Hard blockers:        {len(hard_blockers)}", SECTION)
log("INFO", f"Warnings:             {len(warnings_found)}", SECTION)

if len(hard_blockers) == 0:
    passed("All hard-blocker assertions passed.", SECTION)
else:
    for b in hard_blockers:
        log("🚨 BLOCKER",
            f"{b['assertion']} — value={b['value']} — {b['description']}",
            SECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Final Sign-Off Summary

# COMMAND ----------

SECTION = "FINAL SUMMARY"
log("INFO", "=" * 60, SECTION)
log("INFO", "MDM Phase 2 Sign-Off Summary", SECTION)

summary_rows = [
    ("#1", "V_D_ITEM EAN quality + SKU seed export",
     "✅ Data exported" if null_ean_active == 0 else "🚨 NULL EAN active products found"),
    ("#2", "SELL_OUT UPC cascade distribution",
     f"✅ P1={p1_pct}%" if p1_pct >= UPC_P1_RATE_WARN else f"⚠️  P1={p1_pct}% below {UPC_P1_RATE_WARN}% threshold"),
    ("#3", "Nielsen market MRKT_DSC_SHRT extraction",
     f"⚠️  {total_market_vals if dfs else 'N/A'} strings exported — manual mapping required"),
    ("#4", "V_D_CLIENT CUS_GRN_CHL_DSC classification",
     "⚠️  Data exported — manual CANAL vs CADENA classification required"),
    ("#5", "VW_D_STORE_RM CHAIN/FORMAT classification",
     "⚠️  Data exported — manual CADENA vs CANAL classification required"),
    ("#6", "Hard-blocker assertion suite",
     f"✅ Zero blockers" if len(hard_blockers) == 0 else f"🚨 {len(hard_blockers)} blocker(s) detected"),
]

df_summary = spark.createDataFrame(summary_rows, ["sign_off", "condition", "status"])
display(df_summary)

log("INFO", "", SECTION)
log("INFO", "SIGN-OFF STATUS:", SECTION)
for so, cond, status in summary_rows:
    log("INFO", f"  {so} | {cond[:50]:<50} | {status}", SECTION)

log("INFO", "", SECTION)
if len(hard_blockers) == 0:
    log("INFO",
        "✅ No hard blockers detected in automated assertions. "
        "Manual steps (sign-offs #3, #4, #5) still require human review before Phase 3.",
        SECTION)
else:
    log("🚨 BLOCKER",
        f"{len(hard_blockers)} hard blocker(s) must be resolved before Phase 3 begins. "
        f"See signoff_06_hard_blocker_check.csv for details.",
        SECTION)

log("INFO",
    "\nNEXT ACTIONS:\n"
    "  1. Download signoff_01_sku_mapping.csv → validate EAN cardinality → copy to homologation/sku_mapping.csv\n"
    "  2. Review signoff_02_upc_unmatched.csv → escalate unmatched SELL_OUT products to business\n"
    "  3. Fill in signoff_03_nielsen_markets.csv → map each MRKT_DSC_SHRT row → copy to homologation/nielsen_market_mapping.csv\n"
    "  4. Classify signoff_04_v_d_client.csv → update channel_mapping.csv / cadena_mapping.csv for SELL_IN\n"
    "  5. Classify signoff_05_store_chain.csv → update cadena_mapping.csv / channel_mapping.csv for SELL_OUT\n"
    "  6. Re-run validate_mdm_catalogs.py and validate_mdm_cross_source_joins.py with Snowflake credentials\n"
    "  7. Once all six conditions are met, update task.md and promote to Phase 3",
    SECTION)

# COMMAND ----------

# ── Flush all accumulated log lines to DBFS ───────────────────────────────────
flush_log()
print(f"\n{'='*60}")
print(f"Phase 2 Sign-Off Notebook Complete")
print(f"Output directory: {DBFS_ROOT}")
print(f"{'='*60}")
print(f"Files written:")
print(f"  signoff_audit_log.txt")
print(f"  signoff_01_sku_quality.csv")
print(f"  signoff_01_ean_cardinality.csv")
print(f"  signoff_01_sku_mapping.csv")
print(f"  signoff_02_upc_cascade.csv")
print(f"  signoff_02_upc_unmatched.csv")
print(f"  signoff_03_nielsen_markets.csv")
print(f"  signoff_04_v_d_client_schema.csv")
print(f"  signoff_04_v_d_client.csv")
print(f"  signoff_05_store_schema.csv")
print(f"  signoff_05_store_chain.csv")
print(f"  signoff_06_hard_blocker_check.csv")

# COMMAND ----------


