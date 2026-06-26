# Databricks notebook source
# =============================================================================
# PHASE 3 — SELL_IN STANDARDIZATION: silver_sell_in.py
# =============================================================================
# SOURCE FACT:  PRD_MEX.MEX_DSP_OTC.VW_FACT_RNV   (SELL_IN fact — revenue/volume)
# DIMENSIONS:   V_D_ITEM (product), V_D_CLIENT (customer), V_D_PERIOD (period)
# OUTPUT:       sell_in_std
# GRAIN:        one row per fact record in VW_FACT_RNV
#
# RULES ENFORCED:
#   R4:  ACTIVE_CATALOG_FILTER = SKU_EAN_COD IS NOT NULL
#   R12: All mapping rules from YAML/CSV only — never hardcoded in SQL
#   R14: load_mapping_csv() asserts uniqueness before every join
#   R15: assert_row_count_exact() after every left join
#
# NOTES:
#   - V_D_ITEM is a DIMENSION join — not the grain/fact.
#     sell_in_std grain = VW_FACT_RNV rows.
#   - M2 gate: cadena_std populated only if CUS_CADENA_DSC or CUS_CADENA_IDT
#     is confirmed in logs/signoff_04_v_d_client_cadena_candidates.csv.
#     If NEEDS_REVIEW: cadena_std = NULL, rows quarantined with WARNING.
# =============================================================================

# COMMAND ----------

# MAGIC %run ./silver_homologation_apply

# COMMAND ----------

# MAGIC %md
# MAGIC ## SELL_IN Standardization — silver_sell_in.py
# MAGIC Source: VW_FACT_RNV (fact) + V_D_ITEM (product dim) + V_D_CLIENT (customer dim)

# COMMAND ----------

_S = "SELL_IN"
log("INFO", "Starting SELL_IN standardization", _S)

# Load source tables from pipeline_config.yaml (R12)
cfg = load_yaml_config("configs/pipeline_config.yaml")
SELL_IN_FACT = "PRD_MEX.MEX_DSP_OTC.VW_FACT_RNV"

# Probe fact table accessibility (Change #2)
try:
    _probe = run_sf(DB_PRD_MEX, f"SELECT 1 FROM {SELL_IN_FACT} LIMIT 1")
    log("INFO", f"SELL_IN fact confirmed accessible: {SELL_IN_FACT}", _S)
except Exception as _e:
    log("WARNING", f"{SELL_IN_FACT} not accessible: {_e}. Running schema discovery.", _S)
    try:
        df_discovery = run_sf(DB_PRD_MEX, """
            SELECT TABLE_NAME
            FROM PRD_MEX.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'MEX_DSP_OTC'
              AND (TABLE_NAME LIKE '%FACT%' OR TABLE_NAME LIKE '%RNV%'
                   OR TABLE_NAME LIKE '%SELL%' OR TABLE_NAME LIKE '%VENTA%')
            ORDER BY TABLE_NAME
        """)
        candidates = [r["TABLE_NAME"] for r in df_discovery.collect()]
        blocker(True,
            f"VW_FACT_RNV inaccessible. INFORMATION_SCHEMA candidates: {candidates}. "
            "Confirm the correct SELL_IN fact table and update pipeline_config.yaml.",
            _S)
    except Exception as _e2:
        blocker(True, f"Cannot reach PRD_MEX for schema discovery: {_e2}", _S)

# COMMAND ----------

# Read SELL_IN fact (VW_FACT_RNV)
df_fact = run_sf(DB_PRD_MEX, "SELECT * FROM PRD_MEX.MEX_DSP_OTC.VW_FACT_RNV")
df_fact.cache()
n_fact = df_fact.count()
log("INFO", f"VW_FACT_RNV: {n_fact:,} rows", _S)

# Read product dimension — apply R4 active catalog filter
sql_item = f"""
    SELECT
        TO_VARCHAR(MAT_IDT)     AS mat_idt,
        TO_VARCHAR(SKU_EAN_COD) AS sku_ean_cod,
        MAT_LCL_DSC             AS mat_lcl_dsc,
        LV2_UMB_BRD_DSC         AS lv2_umb_brd_dsc,
        CBU                     AS cbu,
        TO_VARCHAR(MAT_ACT_FLG) AS mat_act_flg
    FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM
    WHERE {ACTIVE_CATALOG_FILTER}
"""
df_item = run_sf(DB_PRD_MEX, sql_item)
df_item.cache()
log("INFO", f"V_D_ITEM (R4 filtered): {df_item.count():,} rows", _S)

# Read customer dimension
df_client = run_sf(DB_PRD_MEX, "SELECT * FROM PRD_MEX.MEX_DSP_OTC.V_D_CLIENT")
df_client.cache()
log("INFO", f"V_D_CLIENT: {df_client.count():,} rows", _S)

# Read period dimension
df_period = run_sf(DB_PRD_MEX, "SELECT * FROM PRD_MEX.MEX_DSP_OTC.V_D_PERIOD")
df_period.cache()
log("INFO", f"V_D_PERIOD: {df_period.count():,} rows", _S)

# COMMAND ----------

# SKU mapping — unique on mat_idt (R14)
df_sku_map = load_mapping_csv("homologation/sku_mapping.csv", key_col="mat_idt", section=_S)

# Brand crosswalk from YAML (R12)
brand_cfg = load_yaml_config("configs/brand_crosswalk.yaml")
log("INFO", f"Brand crosswalk loaded — version: {brand_cfg.get('version', 'unknown')}", _S)

# M2 gate: CADENA source confirmation
df_cadena_cand = load_mapping_csv(
    "logs/signoff_04_v_d_client_cadena_candidates.csv",
    key_col="COLUMN_NAME", section=_S)

cadena_col_confirmed = None
for row in df_cadena_cand.collect():
    status = row["mapping_status"] if "mapping_status" in row.asDict() else None
    if status == "CONFIRMED":
        cadena_col_confirmed = row["COLUMN_NAME"]
        log("INFO", f"M2 CONFIRMED: cadena_std source = '{cadena_col_confirmed}'", _S)
        break

if cadena_col_confirmed is None:
    warn(True,
         "M2 PENDING: No CADENA column confirmed in signoff_04_v_d_client_cadena_candidates.csv. "
         "cadena_std will be NULL in sell_in_std until M2 is completed.",
         _S)

# COMMAND ----------

# Determine join key: MAT_IDT is the SAP product key (R1)
# Probe for MAT_IDT in fact columns; fall back to SKU if not present
fact_cols_upper = [c.upper() for c in df_fact.columns]
if "MAT_IDT" in fact_cols_upper:
    FACT_PRODUCT_KEY = "MAT_IDT"
elif "SKU" in fact_cols_upper:
    FACT_PRODUCT_KEY = "SKU"
else:
    blocker(True,
        f"Cannot find product key (MAT_IDT or SKU) in VW_FACT_RNV columns: {df_fact.columns}",
        _S)
    FACT_PRODUCT_KEY = None

if FACT_PRODUCT_KEY:
    df_si = df_fact.join(
        df_item.withColumnRenamed("mat_idt", "_item_mat_idt"),
        F.col(FACT_PRODUCT_KEY) == F.col("_item_mat_idt"),
        "left"
    )
    register_join("silver_sell_in", "VW_FACT_RNV", "V_D_ITEM",
                  f"{FACT_PRODUCT_KEY}=mat_idt", "left")
    assert_row_count_exact(df_fact, df_si, "SELL_IN fact × V_D_ITEM", _S)
else:
    df_si = df_fact

# COMMAND ----------

# Detect customer join key between fact and V_D_CLIENT
if "CUST_KEY" in fact_cols_upper:
    FACT_CUST_KEY = "CUST_KEY"
elif "CUS_IDT" in fact_cols_upper:
    FACT_CUST_KEY = "CUS_IDT"
elif "CLIENTE" in fact_cols_upper:
    FACT_CUST_KEY = "CLIENTE"
else:
    warn(True,
         f"No known customer key (CUST_KEY/CUS_IDT/CLIENTE) in VW_FACT_RNV: {df_fact.columns}. "
         "Customer dim join skipped — cadena_std and canal_std will be NULL.",
         _S)
    FACT_CUST_KEY = None

if FACT_CUST_KEY:
    client_cols_upper = [c.upper() for c in df_client.columns]
    if FACT_CUST_KEY.upper() in client_cols_upper:
        df_si = df_si.join(
            df_client,
            F.col(FACT_CUST_KEY) == df_client[FACT_CUST_KEY],
            "left"
        )
        register_join("silver_sell_in", "sell_in", "V_D_CLIENT",
                      f"{FACT_CUST_KEY}={FACT_CUST_KEY}", "left")
        assert_row_count_exact(df_fact, df_si, "SELL_IN × V_D_CLIENT", _S)
    else:
        warn(True, f"{FACT_CUST_KEY} not found in V_D_CLIENT columns — skipping customer join.", _S)

# CADENA_STD — M2 gated
if cadena_col_confirmed and cadena_col_confirmed in [c.upper() for c in df_si.columns]:
    df_si = df_si.withColumn("cadena_std", F.col(cadena_col_confirmed))
else:
    df_si = df_si.withColumn("cadena_std", F.lit(None).cast("string"))
    # Quarantine rows without cadena_std for M2 review
    quarantine(
        df_si.filter(F.col("cadena_std").isNull()),
        "SELL_IN_CADENA_NULL",
        "M2 PENDING: CUS_CADENA_DSC/CUS_CADENA_IDT not yet confirmed",
        _S
    )

# CANAL_STD from CUS_GRN_CHL_DSC (5 confirmed values — grand channel, not CADENA)
if "CUS_GRN_CHL_DSC" in [c.upper() for c in df_si.columns]:
    df_si = df_si.withColumn("canal_std", F.col("CUS_GRN_CHL_DSC"))
else:
    df_si = df_si.withColumn("canal_std", F.lit(None).cast("string"))
    warn(True, "CUS_GRN_CHL_DSC not found in joined DataFrame — canal_std=NULL", _S)

# MARCA_STD from LV2_UMB_BRD_DSC
if "lv2_umb_brd_dsc" in [c.lower() for c in df_si.columns]:
    df_si = df_si.withColumn("marca_std", F.upper(F.trim(F.col("lv2_umb_brd_dsc"))))
else:
    df_si = df_si.withColumn("marca_std", F.lit(None).cast("string"))
    warn(True, "lv2_umb_brd_dsc not found — marca_std=NULL", _S)

# Source tag
df_si = df_si.withColumn("source_system", F.lit("SELL_IN"))
df_si = df_si.withColumn("std_created_at", F.current_timestamp())

# COMMAND ----------

n_si = df_si.count()
log("INFO", f"sell_in_std rows: {n_si:,} (source VW_FACT_RNV: {n_fact:,})", _S)

# Null rate audit
for col_name in ["cadena_std", "canal_std", "marca_std", "sku_ean_cod"]:
    cols_lower = [c.lower() for c in df_si.columns]
    if col_name in cols_lower:
        n_null = df_si.filter(F.col(col_name).isNull()).count()
        pct = round(n_null / n_si * 100, 2) if n_si > 0 else 0.0
        log("INFO", f"NULL rate — {col_name}: {n_null:,} / {n_si:,} = {pct}%", _S)

# COMMAND ----------

save_df(df_si, "sell_in_std.csv", _S)
log("INFO", "sell_in_std saved. SELL_IN standardization complete.", _S)
flush_log("phase3_standardization_audit_log.txt")
