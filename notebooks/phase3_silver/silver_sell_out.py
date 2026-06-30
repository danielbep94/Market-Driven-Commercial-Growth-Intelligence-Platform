# Databricks notebook source
# =============================================================================
# PHASE 3 — SELL_OUT STANDARDIZATION: silver_sell_out.py
# =============================================================================
# CONFIRMED SCHEMA (configs/column_types_snapshot.yaml):
#   VW_FACT_SELL_OUT : PER_ID, STORE, UPC, VOL_SELL_OUT, PCS_SELL_OUT,
#                      AMOUNT_SELL_OUT, VOL_INV, PCS_INV, AVG_SELL, BU, CBU_ID
#   VW_D_PRODUCT_RM  : CBU_ID, UPC, INT_ID, IMPORT_ID, NAME, BRAND, CATEGORY,
#                      GLOBAL_STATUS, DESCRIPTION, PACKAGING, MANUFACTURER,
#                      FLAVOR, SKU, SIZE, STATUS
#   VW_D_STORE_RM    : INT_ID, CBU_ID, CBU, STORE_DSC, CHAIN_ID, CHAIN, FORMAT,
#                      STORE_NUM, DELEGATION, CITY, STATE, REGION
#
# KEY FACTS:
#   - VW_FACT_SELL_OUT joins to products via UPC (not INT_ID)
#   - VW_FACT_SELL_OUT joins to stores via STORE = VW_D_STORE_RM.INT_ID
#   - VW_D_STORE_RM has NO NAME column — use STORE_DSC
#   - UPC bridge: fact.UPC = V_D_ITEM.SKU_EAN_COD (SELL_OUT EAN bridge)
#
# SCALE DESIGN (200M+ ROW FACT):
#   Phase A: Pull small dims only (VW_D_PRODUCT_RM ~2,622, VW_D_STORE_RM small)
#   Phase B: Snowflake-side aggregation of VW_FACT_SELL_OUT (200M rows)
#            Joins FACT × STORE in Snowflake — returns small result
#   Phase C: Spark join of aggregated fact to product dim (both small)
#            Apply M3/M4 chain/format mapping
#
# RULES: R5, R10, R11, R14, R15
# =============================================================================

# COMMAND ----------

# MAGIC %run ./silver_homologation_apply

# COMMAND ----------

# MAGIC %md
# MAGIC ## SELL_OUT Standardization — silver_sell_out.py
# MAGIC **Confirmed schema:** `VW_FACT_SELL_OUT.UPC` is the EAN bridge key.
# MAGIC `VW_FACT_SELL_OUT.STORE = VW_D_STORE_RM.INT_ID`.

# COMMAND ----------

_S = "SELL_OUT"
log("INFO", "Starting SELL_OUT standardization (Snowflake-pushdown pattern)", _S)
log("INFO", "VW_FACT_SELL_OUT has 200M+ rows — aggregation runs in Snowflake.", _S)

# COMMAND ----------

# =============================================================================
# PHASE A — Row count probe + small dimension pulls
# =============================================================================
log("INFO", "Phase A: Row count probe + small dimension tables", _S)

# Probe row count (no data transfer)
df_probe = run_sf(DB_PRD_MDP, """
    SELECT
        COUNT(*)                   AS total_rows,
        COUNT(DISTINCT UPC)        AS distinct_upcs,
        COUNT(DISTINCT STORE)      AS distinct_stores,
        MIN(PER_ID)                AS min_period,
        MAX(PER_ID)                AS max_period
    FROM PRD_MDP.MDP_DSP.VW_FACT_SELL_OUT
    WHERE PER_ID >= 20250101
""")
display(df_probe)
probe = df_probe.collect()[0]
log("INFO",
    f"VW_FACT_SELL_OUT (2025+): {probe['TOTAL_ROWS']:,} rows | "
    f"{probe['DISTINCT_UPCS']:,} UPCs | {probe['DISTINCT_STORES']:,} stores | "
    f"period: {probe['MIN_PERIOD']} → {probe['MAX_PERIOD']}", _S)

# COMMAND ----------

# VW_D_PRODUCT_RM — small dim (~2,622 rows), NAME column confirmed
df_product = run_sf(DB_PRD_MDP, """
    SELECT
        TO_VARCHAR(UPC)       AS upc,
        TO_VARCHAR(INT_ID)    AS sell_out_int_id,
        TO_VARCHAR(IMPORT_ID) AS sell_out_import_id,
        NAME                  AS so_name,
        BRAND                 AS so_brand,
        CATEGORY              AS so_category,
        CBU_ID
    FROM PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM
    WHERE UPC IS NOT NULL AND TRIM(TO_VARCHAR(UPC)) <> ''
""")
df_product.cache()
n_product = df_product.count()
log("INFO", f"VW_D_PRODUCT_RM: {n_product:,} rows with non-null UPC", _S)

# Deduplicate product dim on INT_ID (sell_out_int_id) — prevent fanout at join time (R14)
# VW_D_PRODUCT_RM may have multiple rows per INT_ID (e.g. multi-CBU duplicates)
# Use MIN(CBU_ID) to deterministically pick one row per INT_ID
df_product_dedup = df_product.groupBy("sell_out_int_id").agg(
    F.min("so_name").alias("so_name"),
    F.min("so_brand").alias("so_brand"),
    F.min("so_category").alias("so_category"),
    F.min("CBU_ID").alias("CBU_ID")
)
n_product_dedup = df_product_dedup.count()
dup_removed = n_product - n_product_dedup
if dup_removed > 0:
    log("INFO",
        f"VW_D_PRODUCT_RM dedup: {n_product:,} → {n_product_dedup:,} rows "
        f"({dup_removed:,} INT_ID duplicates removed, R14)", _S)
else:
    log("INFO", f"VW_D_PRODUCT_RM: no INT_ID duplicates found ({n_product_dedup:,} rows)", _S)

# VW_D_STORE_RM — CONFIRMED: no NAME column, use STORE_DSC
df_store = run_sf(DB_PRD_MDP, """
    SELECT
        TO_VARCHAR(INT_ID) AS store_int_id,
        CHAIN,
        FORMAT,
        STORE_DSC          AS store_name,
        REGION,
        CITY,
        STATE
    FROM PRD_MDP.MDP_DSP.VW_D_STORE_RM
""")
df_store.cache()
n_store = df_store.count()
log("INFO", f"VW_D_STORE_RM: {n_store:,} stores", _S)

# EAN dedup from V_D_ITEM — MIN(MAT_IDT) GROUP BY SKU_EAN_COD (R10)
# UPC in VW_FACT_SELL_OUT maps to SKU_EAN_COD in V_D_ITEM
df_ean_dedup = run_sf(DB_PRD_MEX, """
    SELECT
        TO_VARCHAR(SKU_EAN_COD)  AS sku_ean_cod,
        MIN(TO_VARCHAR(MAT_IDT)) AS mat_idt,
        MIN(MAT_LCL_DSC)         AS si_description
    FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM
    WHERE SKU_EAN_COD IS NOT NULL
      AND TRIM(TO_VARCHAR(SKU_EAN_COD)) <> ''
    GROUP BY TO_VARCHAR(SKU_EAN_COD)
""")
df_ean_dedup.cache()
n_ean = df_ean_dedup.count()
log("INFO", f"EAN dedup: {n_ean:,} unique EANs → 1 MAT_IDT each (R10)", _S)

# Assert uniqueness (R14)
dup_ean = n_ean - df_ean_dedup.dropDuplicates(["sku_ean_cod"]).count()
blocker(dup_ean > 0, f"EAN dedup has {dup_ean} duplicates — P1 join would fanout.", _S)

# COMMAND ----------

# =============================================================================
# PHASE B — Snowflake-side aggregation: FACT × STORE join in Snowflake
# Returns one row per (UPC × chain × format × year_month) — small result
# =============================================================================
log("INFO", "Phase B: Snowflake-side aggregation of VW_FACT_SELL_OUT (200M+ rows)", _S)
log("INFO", "FACT joins to STORE on STORE=INT_ID in Snowflake. UPC is the product key.", _S)

sql_so_agg = """
SELECT
    TO_VARCHAR(f.UPC)             AS upc,
    TO_VARCHAR(f.STORE)           AS store_id,
    s.CHAIN                       AS chain,
    s.FORMAT                      AS format,
    
    s.REGION                      AS region,
    LEFT(TO_VARCHAR(f.PER_ID), 6) AS year_month,

    SUM(COALESCE(f.AMOUNT_SELL_OUT, 0)) AS revenue_sell_out,
    SUM(COALESCE(f.VOL_SELL_OUT, 0))    AS vol_sell_out,
    SUM(COALESCE(f.PCS_SELL_OUT, 0))    AS pcs_sell_out,
    SUM(COALESCE(f.VOL_INV, 0))         AS vol_inv,
    SUM(COALESCE(f.PCS_INV, 0))         AS pcs_inv,
    COUNT(*)                            AS fact_row_count

FROM PRD_MDP.MDP_DSP.VW_FACT_SELL_OUT f
LEFT JOIN PRD_MDP.MDP_DSP.VW_D_STORE_RM s
    ON TO_VARCHAR(f.STORE) = TO_VARCHAR(s.INT_ID)

WHERE f.PER_ID >= 20250101

GROUP BY
    TO_VARCHAR(f.UPC),
    TO_VARCHAR(f.STORE),
    s.CHAIN,
    s.FORMAT,
    s.REGION,
    LEFT(TO_VARCHAR(f.PER_ID), 6)
"""

log("INFO", "Executing Snowflake pushdown — estimated 5–10 min", _S)
df_so_agg = run_sf(DB_PRD_MDP, sql_so_agg)
df_so_agg.cache()
n_so_agg = df_so_agg.count()
log("INFO", f"Aggregated SELL_OUT from Snowflake: {n_so_agg:,} rows", _S)
register_join("silver_sell_out", "VW_FACT_SELL_OUT", "VW_D_STORE_RM",
              "STORE=INT_ID (pushdown in Snowflake)", "left")

# COMMAND ----------

# =============================================================================
# PHASE C — UPC bridge on aggregated fact + product dim (Spark — both small)
# VW_FACT_SELL_OUT.UPC → V_D_ITEM.SKU_EAN_COD → MAT_IDT (EAN bridge)
# =============================================================================
log("INFO", "Phase C: UPC bridge — fact.UPC = V_D_ITEM.SKU_EAN_COD", _S)

# Join aggregated fact to EAN dedup (fact.upc = ean_dedup.sku_ean_cod)
df_so = df_so_agg.join(
    df_ean_dedup.select(
        F.col("sku_ean_cod"),
        F.col("mat_idt"),
        F.col("si_description")),
    df_so_agg["upc"] == df_ean_dedup["sku_ean_cod"],
    "left")
register_join("silver_sell_out", "sell_out_agg", "V_D_ITEM_ean_dedup",
              "upc=sku_ean_cod", "left")
assert_row_count_exact(df_so_agg, df_so, "SELL_OUT agg × EAN dedup", _S)

# A5/A6: fanout assertion — each UPC should map to at most 1 MAT_IDT
df_fanout = (df_so.filter(F.col("mat_idt").isNotNull())
               .groupBy("upc")
               .agg(F.countDistinct("mat_idt").alias("n_mat"))
               .filter(F.col("n_mat") > 1))
n_fanout = df_fanout.count()
blocker(n_fanout > 0,
    f"A5/A6: {n_fanout} UPCs map to >1 MAT_IDT — EAN dedup failed.", _S)
if n_fanout == 0:
    passed("A5/A6: 0 UPC fanouts — 1:1 UPC→MAT_IDT confirmed", _S)

# P3: unmatched UPCs → quarantine (R11)
n_unmatched = df_so.filter(F.col("mat_idt").isNull()).select("upc").distinct().count()
if n_unmatched > 0:
    quarantine(df_so.filter(F.col("mat_idt").isNull()),
               "SELL_OUT_UPC_NO_EAN_MATCH",
               f"{n_unmatched:,} distinct UPCs in SELL_OUT not found in V_D_ITEM EAN catalog",
               _S)
    warn(True, f"{n_unmatched:,} SELL_OUT UPCs have no match in V_D_ITEM", _S)
else:
    passed("All SELL_OUT UPCs matched to V_D_ITEM EAN catalog", _S)

# Join product dim (VW_D_PRODUCT_RM) for brand/name enrichment — use deduped table (R14 anti-fanout)
# IMPORTANT: VW_FACT_SELL_OUT.UPC = internal product code = VW_D_PRODUCT_RM.INT_ID
# VW_D_PRODUCT_RM.UPC is the EAN barcode — joining upc=upc produces 0 matches
df_so = df_so.join(
    df_product_dedup.select(
        F.col("sell_out_int_id").alias("upc_key"),  # INT_ID matches FACT.UPC
        F.col("so_name"), F.col("so_brand"), F.col("so_category"), F.col("CBU_ID")),
    df_so["upc"] == F.col("upc_key"), "left")
register_join("silver_sell_out", "sell_out", "VW_D_PRODUCT_RM_dedup",
              "upc=sell_out_int_id (FACT.UPC=PRODUCT.INT_ID, deduped)", "left")
assert_row_count_exact(df_so_agg, df_so, "SELL_OUT × VW_D_PRODUCT_RM", _S)

# COMMAND ----------

# =============================================================================
# PHASE D — Apply Enterprise Channel Hierarchy (M3/M4 Replacement)
# =============================================================================
log("INFO", "Phase D: Applying enterprise hierarchy from seed", _S)

# Read seed directly and filter to SELL_OUT BEFORE uniqueness check.
# NOTE: load_mapping_csv checks uniqueness across the entire file — but source_value
# (e.g. "MODERNO", "UTT") is intentionally repeated across source systems (WASTE, NIELSEN, IBP).
# We must filter first, then validate uniqueness within SELL_OUT scope only.
_seed_path = os.path.join(REPO_ROOT, "configs", "catalog_seeds", "channel_hierarchy_seed.csv")
df_seed_raw = spark.read.csv(f"file:{_seed_path}", header=True, inferSchema=False)

df_seed_so = df_seed_raw.filter(
    (F.upper(F.trim(F.col("source_system"))) == "SELL_OUT") &
    (F.upper(F.trim(F.col("mapping_status"))) == "CONFIRMED")
).select(
    F.upper(F.trim(F.col("source_value"))).alias("source_value"),
    F.upper(F.trim(F.col("gran_canal_grp"))).alias("gran_canal_grp"),
    F.upper(F.trim(F.col("channel_standard"))).alias("channel_standard"),
    F.upper(F.trim(F.col("chain_standard"))).alias("chain_standard"),
    F.upper(F.trim(F.col("format_standard"))).alias("format_standard"),
)

# Assert uniqueness within SELL_OUT scope
n_seed_so  = df_seed_so.count()
n_seed_uniq = df_seed_so.dropDuplicates(["source_value"]).count()
blocker(n_seed_so != n_seed_uniq,
        f"SELL_OUT seed has {n_seed_so - n_seed_uniq} duplicate source_value entries — fix seed before joining.",
        _S)
log("INFO", f"SELL_OUT seed validated: {n_seed_so} unique FORMAT mappings", _S)

df_seed_so.cache()

df_so = df_so.join(
    df_seed_so,
    F.upper(F.trim(df_so["format"])) == df_seed_so["source_value"],
    "left"
)
register_join("silver_sell_out", "sell_out", "channel_hierarchy_seed[SELL_OUT]",
              "UPPER(format)=source_value", "left")
assert_row_count_exact(df_so_agg, df_so, "SELL_OUT × enterprise seed", _S)

# Validation gates — strict enterprise vocabulary enforcement
if n_so_agg > 0:
    valid_channels = {"MODERNO", "TRADICIONAL", "INTERNOS"}
    valid_gran     = {"UTT", "DTT", "INTERNAL"}

    actual_channels = set(df_so.select("channel_standard").dropna().distinct().toPandas()["channel_standard"])
    actual_gran     = set(df_so.select("gran_canal_grp").dropna().distinct().toPandas()["gran_canal_grp"])

    assert actual_channels <= valid_channels, f"Invalid channel_standard found: {actual_channels - valid_channels}"
    assert actual_gran     <= valid_gran,     f"Invalid gran_canal_grp found: {actual_gran - valid_gran}"

# Quarantine unmapped FORMATs for seed expansion
quarantine(df_so.filter(F.col("chain_standard").isNull()),
           "SELL_OUT_CHAIN_NEEDS_REVIEW",
           "PENDING: FORMAT not yet mapped to chain_standard", _S)
quarantine(df_so.filter(F.col("channel_standard").isNull()),
           "SELL_OUT_CANAL_NEEDS_REVIEW",
           "PENDING: FORMAT not yet mapped to channel_standard", _S)

# marca_std = normalized brand from VW_D_PRODUCT_RM.BRAND (so_brand)
# Upper+trim for consistency with sell_in_std.marca_std and other sources
df_so = df_so.withColumn("marca_std", F.upper(F.trim(F.col("so_brand"))))
log("INFO", "marca_std derived from so_brand (UPPER TRIM)", _S)

df_so = df_so.withColumn("source_system", F.lit("SELL_OUT")) \
             .withColumn("std_created_at", F.current_timestamp())


# COMMAND ----------

# Null rate audit + save
n_so = df_so.count()
log("INFO", f"sell_out_std final: {n_so:,} rows", _S)
for col_name in ["mat_idt", "chain_standard", "channel_standard", "revenue_sell_out", "so_brand"]:
    if col_name in [c.lower() for c in df_so.columns]:
        n_null = df_so.filter(F.col(col_name).isNull()).count()
        pct = round(n_null / n_so * 100, 2) if n_so > 0 else 0.0
        log("INFO", f"NULL rate — {col_name}: {n_null:,} / {n_so:,} = {pct}%", _S)

save_df(df_so, "sell_out_std.csv", _S)
log("INFO", "sell_out_std saved. SELL_OUT standardization complete.", _S)
flush_log("phase3_standardization_audit_log.txt")

# COMMAND ----------


