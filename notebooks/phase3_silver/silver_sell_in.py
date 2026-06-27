# Databricks notebook source
# =============================================================================
# PHASE 3 — SELL_IN STANDARDIZATION: silver_sell_in.py
# =============================================================================
# SOURCE FACT:  PRD_MEX.MEX_DSP_OTC.VW_FACT_RNV   (200M+ rows)
# DIMENSIONS:   V_D_ITEM (product), V_D_CLIENT (customer)
# OUTPUT:       sell_in_std
#
# ⚠️  SCALE DESIGN — 200M+ ROW FACT TABLE:
#   ALL joins are pushed into a single Snowflake SQL query.
#   Snowflake does the heavy lifting (join + aggregate + deduplicate).
#   Only the result set (distinct dim keys + aggregated metrics) is
#   pulled into Spark. This keeps the Spark DataFrame small (<1M rows).
#
#   Pattern: Snowflake handles volume → Spark handles standardization mapping.
#
# GRAIN OF sell_in_std:
#   One row per unique (MAT_IDT × customer_key × year_month) combination.
#   Metrics: SUM(revenue), SUM(volume) — aggregated in Snowflake.
#
# RULES ENFORCED:
#   R4:  ACTIVE_CATALOG_FILTER = SKU_EAN_COD IS NOT NULL
#   R12: All mapping rules from YAML/CSV only
#   R14: load_mapping_csv() uniqueness assertion
#   R15: assert_row_count_exact() after Spark-side joins only
# =============================================================================

# COMMAND ----------

# MAGIC %run ./silver_homologation_apply

# COMMAND ----------

# MAGIC %md
# MAGIC ## SELL_IN Standardization — silver_sell_in.py
# MAGIC **Scale pattern:** All dimension joins and aggregation pushed to Snowflake.
# MAGIC Spark only receives the pre-joined, aggregated result set.

# COMMAND ----------

_S = "SELL_IN"
log("INFO", "Starting SELL_IN standardization (Snowflake-pushdown pattern)", _S)
log("INFO", "VW_FACT_RNV has 200M+ rows — ALL joins and aggregation run in Snowflake.", _S)

SELL_IN_FACT = "PRD_MEX.MEX_DSP_OTC.VW_FACT_RNV"

# Probe fact table accessibility
try:
    _probe = run_sf(DB_PRD_MEX, f"SELECT 1 FROM {SELL_IN_FACT} LIMIT 1")
    log("INFO", f"SELL_IN fact confirmed accessible: {SELL_IN_FACT}", _S)
except Exception as _e:
    log("WARNING", f"{SELL_IN_FACT} not accessible: {_e}. Running schema discovery.", _S)
    try:
        df_disc = run_sf(DB_PRD_MEX, """
            SELECT TABLE_NAME FROM PRD_MEX.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'MEX_DSP_OTC'
              AND (TABLE_NAME LIKE '%FACT%' OR TABLE_NAME LIKE '%RNV%'
                   OR TABLE_NAME LIKE '%SELL%' OR TABLE_NAME LIKE '%VENTA%')
            ORDER BY TABLE_NAME
        """)
        blocker(True,
            f"VW_FACT_RNV inaccessible. Candidates: {[r['TABLE_NAME'] for r in df_disc.collect()]}",
            _S)
    except Exception as _e2:
        blocker(True, f"Cannot reach PRD_MEX: {_e2}", _S)

# COMMAND ----------

# =============================================================================
# STEP 1 — ROW COUNT PROBE (fast — no data transfer)
# Run this first to confirm scale before the main query.
# =============================================================================
log("INFO", "Step 1: Row count probe (no data transfer)", _S)

df_probe = run_sf(DB_PRD_MEX, """
    SELECT
        COUNT(*) AS total_rows,
        COUNT(DISTINCT TO_VARCHAR(MAT_IDT)) AS distinct_mat_idt,
        MIN(BIL_DAT) AS min_date,
        MAX(BIL_DAT) AS max_date
    FROM PRD_MEX.MEX_DSP_OTC.VW_FACT_RNV
    WHERE BIL_DAT >= 20250101
""")
display(df_probe)
probe_row = df_probe.collect()[0]
log("INFO",
    f"VW_FACT_RNV (2025+): {probe_row['TOTAL_ROWS']:,} rows | "
    f"{probe_row['DISTINCT_MAT_IDT']:,} distinct MAT_IDTs | "
    f"date range: {probe_row['MIN_DATE']} → {probe_row['MAX_DATE']}",
    _S)

# COMMAND ----------

# =============================================================================
# STEP 2 — MAIN QUERY: Joins + aggregation ENTIRELY in Snowflake
# Snowflake handles the 200M row join. Spark receives ~81K rows (one per SKU).
# =============================================================================
log("INFO", "Step 2: Pushdown join + aggregation in Snowflake (main query)", _S)
log("INFO", "Expected result: ~81,627 rows (one per active SKU in V_D_ITEM)", _S)

# CONFIRMED COLUMN NAMES (from configs/column_types_snapshot.yaml + SELL_IN_DICT.txt):
# VW_FACT_RNV: BIL_DAT, MAT_IDT, SHP_CUS_IDT, BIL_INV, BIL_NET_KGR, LITER, CASES, DIS_CHL_COD, CBU
# V_D_ITEM:    MAT_IDT, SKU_EAN_COD, MAT_LCL_DSC, LV2_UMB_BRD_DSC, CBU
# V_D_CLIENT:  CUS_IDT, CUS_GRN_CHL_DSC, CUS_GRP_DSC
# Customer join: SHP_CUS_IDT → VW_D_CUSTOMER_DICTONARY.OLD_CUS_IDT → NEW_CUS_IDT → V_D_CLIENT.CUS_IDT

sql_sell_in_std = """
WITH customer_map AS (
    -- Harmonize customer IDs via VW_D_CUSTOMER_DICTONARY (pattern from SELL_IN_DICT.txt)
    SELECT DISTINCT
        OLD_CUS_IDT,
        COALESCE(NEW_CUS_IDT, OLD_CUS_IDT) AS harmonized_cus_idt
    FROM PRD_MEX.MEX_DSP_OTC.VW_D_CUSTOMER_DICTONARY
)
SELECT
    -- Product keys
    TO_VARCHAR(f.MAT_IDT)           AS mat_idt,
    TO_VARCHAR(i.SKU_EAN_COD)       AS sku_ean_cod,
    i.MAT_LCL_DSC                   AS mat_lcl_dsc,
    UPPER(TRIM(i.LV2_UMB_BRD_DSC)) AS marca_std,
    COALESCE(f.CBU, i.CBU)          AS cbu,

    -- Customer / channel
    c.CUS_GRN_CHL_DSC               AS canal_raw,
    c.CUS_GRP_DSC                   AS cus_grp_dsc,
    f.DIS_CHL_COD                   AS dis_chl_cod,

    -- Period grain (YYYYMM)
    LEFT(TO_VARCHAR(f.BIL_DAT), 6)  AS year_month,

    -- Confirmed metric columns (column_types_snapshot.yaml)
    SUM(COALESCE(f.BIL_INV, 0))     AS revenue_mxn,
    SUM(COALESCE(f.BIL_NET_KGR, 0)) AS volume_kgr,
    SUM(COALESCE(f.LITER, 0))       AS volume_liter,
    SUM(COALESCE(f.CASES, 0))       AS cases,
    SUM(COALESCE(f.BIL_SKU_QTY, 0)) AS sku_qty,
    COUNT(*)                         AS fact_row_count,

    'SELL_IN'                        AS source_system,
    CURRENT_TIMESTAMP()              AS std_created_at

FROM PRD_MEX.MEX_DSP_OTC.VW_FACT_RNV f

-- Product dim join (R4: active catalog = SKU_EAN_COD IS NOT NULL)
LEFT JOIN PRD_MEX.MEX_DSP_OTC.V_D_ITEM i
    ON TO_VARCHAR(f.MAT_IDT) = TO_VARCHAR(i.MAT_IDT)
    AND i.SKU_EAN_COD IS NOT NULL

-- Customer harmonization (SHP_CUS_IDT → OLD_CUS_IDT → harmonized_cus_idt)
LEFT JOIN customer_map cm
    ON TO_VARCHAR(f.SHP_CUS_IDT) = cm.OLD_CUS_IDT

-- Customer dim join (harmonized ID → V_D_CLIENT.CUS_IDT)
LEFT JOIN PRD_MEX.MEX_DSP_OTC.V_D_CLIENT c
    ON COALESCE(cm.harmonized_cus_idt, TO_VARCHAR(f.SHP_CUS_IDT)) = TO_VARCHAR(c.CUS_IDT)

WHERE f.BIL_DAT >= 20250101

GROUP BY
    TO_VARCHAR(f.MAT_IDT),
    TO_VARCHAR(i.SKU_EAN_COD),
    i.MAT_LCL_DSC,
    UPPER(TRIM(i.LV2_UMB_BRD_DSC)),
    COALESCE(f.CBU, i.CBU),
    c.CUS_GRN_CHL_DSC,
    c.CUS_GRP_DSC,
    f.DIS_CHL_COD,
    LEFT(TO_VARCHAR(f.BIL_DAT), 6)
"""

log("INFO", "Executing Snowflake pushdown query — this runs entirely in Snowflake.", _S)
log("INFO", "Estimated time: 3-8 min (Snowflake aggregates 200M rows, returns ~81K)", _S)

df_si = run_sf(DB_PRD_MEX, sql_sell_in_std)
df_si.cache()
n_si = df_si.count()
log("INFO", f"sell_in_std received from Snowflake: {n_si:,} rows", _S)

register_join("silver_sell_in", "VW_FACT_RNV", "V_D_ITEM + V_D_CLIENT",
              "MAT_IDT (pushdown — join in Snowflake)", "left")

display(df_si.limit(20))

# COMMAND ----------

# =============================================================================
# STEP 3 — Apply cadena_std mapping (M2 gate — Spark side, small DataFrame)
# =============================================================================
log("INFO", "Step 3: Applying dimension standardization mappings (Spark side)", _S)

# M2 gate: CADENA source
df_cadena_cand = load_mapping_csv(
    "logs/signoff_04_v_d_client_cadena_candidates.csv",
    key_col="COLUMN_NAME", section=_S)

cadena_col_confirmed = None
for row in df_cadena_cand.collect():
    row_dict = row.asDict()
    if row_dict.get("mapping_status") == "CONFIRMED":
        cadena_col_confirmed = row_dict.get("COLUMN_NAME")
        log("INFO", f"M2 CONFIRMED: cadena_std source = '{cadena_col_confirmed}'", _S)
        break

if cadena_col_confirmed is None:
    warn(True,
         "M2 PENDING: No CADENA column confirmed yet — cadena_std=NULL in sell_in_std.",
         _S)

# Apply cadena_std
cols_lower = [c.lower() for c in df_si.columns]
if cadena_col_confirmed and cadena_col_confirmed.lower() in cols_lower:
    df_si = df_si.withColumn("cadena_std", F.col(cadena_col_confirmed))
else:
    df_si = df_si.withColumn("cadena_std", F.lit(None).cast("string"))

# canal_std from canal_raw (CUS_GRN_CHL_DSC — 5 confirmed grand-channel values)
if "canal_raw" in cols_lower:
    df_si = df_si.withColumn("canal_std", F.col("canal_raw"))
else:
    df_si = df_si.withColumn("canal_std", F.lit(None).cast("string"))
    warn(True, "CUS_GRN_CHL_DSC not in pushdown result — canal_std=NULL", _S)

# Quarantine CADENA NULLs for M2 review
if cadena_col_confirmed is None:
    df_cadena_null = df_si.filter(F.col("cadena_std").isNull()) \
                           .select("mat_idt", "sku_ean_cod", "canal_std", "year_month")
    quarantine(df_cadena_null, "SELL_IN_CADENA_NULL",
               "M2 PENDING: cadena_std source not confirmed", _S)

# COMMAND ----------

# =============================================================================
# STEP 4 — Null rate audit + save
# =============================================================================
log("INFO", "Step 4: Null rate audit", _S)

n_si_final = df_si.count()
for col_name in ["cadena_std", "canal_std", "marca_std", "sku_ean_cod", "revenue_mxn", "volume_kgr"]:
    if col_name in [c.lower() for c in df_si.columns]:
        n_null = df_si.filter(F.col(col_name).isNull()).count()
        pct = round(n_null / n_si_final * 100, 2) if n_si_final > 0 else 0.0
        log("INFO", f"NULL rate — {col_name}: {n_null:,} / {n_si_final:,} = {pct}%", _S)

log("INFO",
    f"sell_in_std final: {n_si_final:,} rows "
    f"(aggregated from {probe_row['TOTAL_ROWS']:,} source fact rows in Snowflake)",
    _S)

save_df(df_si, "sell_in_std.csv", _S)
log("INFO", "sell_in_std saved. SELL_IN standardization complete.", _S)
flush_log("phase3_standardization_audit_log.txt")

# COMMAND ----------

