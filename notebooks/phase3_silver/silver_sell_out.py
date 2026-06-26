# Databricks notebook source
# =============================================================================
# PHASE 3 — SELL_OUT STANDARDIZATION: silver_sell_out.py
# =============================================================================
# SOURCE FACT:  PRD_MDP.MDP_DSP.VW_FACT_SELL_OUT   (200M+ rows)
# DIMENSIONS:   VW_D_PRODUCT_RM, VW_D_STORE_RM
# BRIDGE:       V_D_ITEM EAN dedup (PRD_MEX) — UPC bridge only (R5)
# OUTPUT:       sell_out_std
#
# ⚠️  SCALE DESIGN — 200M+ ROW FACT TABLE:
#   Two-phase Snowflake pushdown:
#   Phase A: Pull small dimension tables only (VW_D_PRODUCT_RM ~2,622 rows,
#            VW_D_STORE_RM ~hundreds of rows, V_D_ITEM EAN dedup ~2,940 rows)
#   Phase B: Spark-side join of small dimensions + UPC bridge (fast).
#            Result = enriched product mapping table (not 200M rows).
#   Phase C: Aggregated fact metrics pulled from Snowflake using a pushdown
#            GROUP BY query — Snowflake aggregates 200M → small result set.
#
# GRAIN OF sell_out_std:
#   One row per (sell_out_int_id × store_chain × year_month).
#   Metrics: SUM(amount_sell_out), SUM(vol_sell_out) — aggregated in Snowflake.
#
# BRIDGE TIERS (R5, R10, R11):
#   P1: INT_ID = SKU_EAN_COD  (match_priority=1)
#   P2: IMPORT_ID = SKU_EAN_COD (match_priority=2)
#   P3: UNMATCHED → quarantine (R11)
#
# RULES: R5, R10, R11, R14, R15
# =============================================================================

# COMMAND ----------

# MAGIC %run ./silver_homologation_apply

# COMMAND ----------

# MAGIC %md
# MAGIC ## SELL_OUT Standardization — silver_sell_out.py
# MAGIC **Scale pattern:** Dimension tables pulled small. Fact aggregated in Snowflake.
# MAGIC Never pulls 200M rows into Spark.

# COMMAND ----------

_S = "SELL_OUT"
log("INFO", "Starting SELL_OUT standardization (Snowflake-pushdown pattern)", _S)
log("INFO", "VW_FACT_SELL_OUT has 200M+ rows — aggregation runs in Snowflake.", _S)

# COMMAND ----------

# =============================================================================
# PHASE A — Pull small dimension tables into Spark (fast)
# These are small: products ~2,622 rows, stores ~hundreds, EAN dedup ~2,940 rows
# =============================================================================
log("INFO", "Phase A: Loading small dimension tables into Spark", _S)

# SELL_OUT product dimension (~2,622 rows — small)
df_product = run_sf(DB_PRD_MDP, """
    SELECT
        TO_VARCHAR(INT_ID)    AS sell_out_int_id,
        TO_VARCHAR(IMPORT_ID) AS sell_out_import_id,
        NAME                  AS so_name,
        BRAND                 AS so_brand,
        CBU_ID
    FROM PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM
""")
df_product.cache()
n_product = df_product.count()
log("INFO", f"VW_D_PRODUCT_RM: {n_product:,} rows (small — in Spark memory)", _S)

# SELL_OUT store dimension (small)
df_store = run_sf(DB_PRD_MDP, """
    SELECT
        TO_VARCHAR(INT_ID) AS store_int_id,
        CHAIN,
        FORMAT,
        NAME               AS store_name
    FROM PRD_MDP.MDP_DSP.VW_D_STORE_RM
""")
df_store.cache()
n_store = df_store.count()
log("INFO", f"VW_D_STORE_RM: {n_store:,} rows (small — in Spark memory)", _S)

# EAN dedup from V_D_ITEM — MIN(MAT_IDT) GROUP BY EAN (R10) — ~2,940 rows
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
log("INFO", f"EAN dedup table: {n_ean:,} unique EANs (one MAT_IDT per EAN, R10)", _S)

# Assert uniqueness of EAN dedup (R14)
dup_ean = n_ean - df_ean_dedup.dropDuplicates(["sku_ean_cod"]).count()
blocker(dup_ean > 0,
    f"EAN dedup has {dup_ean} duplicate sku_ean_cod values — P1 join would fanout.",
    _S)

# COMMAND ----------

# =============================================================================
# PHASE B — UPC bridge on small dimensions (Spark side — fast)
# P1: INT_ID = EAN, P2: IMPORT_ID = EAN, P3: unmatched
# =============================================================================
log("INFO", "Phase B: Building UPC bridge on small dimension tables (Spark)", _S)

# P1: INT_ID = EAN
df_p1 = (df_product
         .join(df_ean_dedup,
               df_product["sell_out_int_id"] == df_ean_dedup["sku_ean_cod"],
               "inner")
         .withColumn("match_priority",   F.lit(1))
         .withColumn("match_method",     F.lit("EXACT_INT_ID"))
         .withColumn("match_confidence", F.lit(1.0))
         .withColumn("review_status",    F.lit("CONFIRMED")))
register_join("silver_sell_out", "VW_D_PRODUCT_RM", "V_D_ITEM_ean_dedup",
              "sell_out_int_id=sku_ean_cod", "inner")

p1_matched_int_ids = set(r["sell_out_int_id"]
                         for r in df_p1.select("sell_out_int_id").distinct().collect())
n_p1 = df_p1.count()
log("INFO", f"P1 matches: {n_p1:,} products via INT_ID", _S)

# A5/A6 fanout assertion
df_p1_fanout = (df_p1.groupBy("sku_ean_cod")
                .agg(F.countDistinct("mat_idt").alias("distinct_mat_idt"))
                .filter(F.col("distinct_mat_idt") > 1))
n_fanout = df_p1_fanout.count()
blocker(n_fanout > 0,
    f"P1 bridge has {n_fanout} EANs mapping to >1 MAT_IDT — EAN dedup failed.", _S)
if n_fanout == 0:
    passed("P1 bridge: 0 ambiguous EANs — 1:1 guarantee confirmed (A5, A6)", _S)

# P2: IMPORT_ID = EAN (exclude P1)
df_p2_cand = df_product.filter(~F.col("sell_out_int_id").isin(p1_matched_int_ids))
df_p2 = (df_p2_cand
         .join(df_ean_dedup,
               df_p2_cand["sell_out_import_id"] == df_ean_dedup["sku_ean_cod"],
               "inner")
         .withColumn("match_priority",   F.lit(2))
         .withColumn("match_method",     F.lit("EXACT_IMPORT_ID"))
         .withColumn("match_confidence", F.lit(0.9))
         .withColumn("review_status",    F.lit("CONFIRMED")))
register_join("silver_sell_out", "VW_D_PRODUCT_RM_p2", "V_D_ITEM_ean_dedup",
              "sell_out_import_id=sku_ean_cod", "inner")
n_p2 = df_p2.count()
log("INFO", f"P2 matches: {n_p2:,} products via IMPORT_ID", _S)

# P3: Unmatched → quarantine (R11)
p2_matched_import_ids = set(r["sell_out_import_id"]
                            for r in df_p2.select("sell_out_import_id").distinct().collect())
df_unmatched = (df_product
                .filter(~F.col("sell_out_int_id").isin(p1_matched_int_ids))
                .filter(~F.col("sell_out_import_id").isin(p2_matched_import_ids)
                        if p2_matched_import_ids else F.lit(True))
                .withColumn("mat_idt",        F.lit(None).cast("string"))
                .withColumn("sku_ean_cod",    F.lit(None).cast("string"))
                .withColumn("match_priority", F.lit(3))
                .withColumn("match_method",   F.lit("UNMATCHED"))
                .withColumn("review_status",  F.lit("NEEDS_REVIEW")))
n_unmatched = df_unmatched.count()
quarantine(df_unmatched, "SELL_OUT_P3_UNMATCHED",
           "No EAN match via INT_ID or IMPORT_ID", _S)

# Cascade summary
p1_pct = round(n_p1 / n_product * 100, 2) if n_product > 0 else 0.0
p2_pct = round(n_p2 / n_product * 100, 2) if n_product > 0 else 0.0
um_pct = round(n_unmatched / n_product * 100, 2) if n_product > 0 else 0.0
log("INFO",
    f"UPC Cascade: P1={n_p1:,} ({p1_pct}%) | P2={n_p2:,} ({p2_pct}%) "
    f"| Unmatched={n_unmatched:,} ({um_pct}%)", _S)
warn(p1_pct < 70.0, f"P1 rate {p1_pct}% below 70% threshold", _S)
if p1_pct >= 70.0:
    passed(f"P1 match rate {p1_pct}% >= 70%", _S)

# COMMAND ----------

# =============================================================================
# PHASE C — Aggregated fact from Snowflake (pushdown — small result)
# Snowflake aggregates 200M rows. Spark receives one row per
# (sell_out_int_id × store_chain × year_month).
# =============================================================================
log("INFO", "Phase C: Snowflake-side aggregation of VW_FACT_SELL_OUT (200M rows)", _S)
log("INFO", "Estimated time: 5-10 min (Snowflake aggregates internally)", _S)

sql_so_agg = """
SELECT
    TO_VARCHAR(f.INT_ID)          AS sell_out_int_id,
    TO_VARCHAR(f.STORE)           AS store_id,
    s.CHAIN                       AS chain,
    s.FORMAT                      AS format,
    LEFT(TO_VARCHAR(f.PER_ID), 6) AS year_month,

    SUM(COALESCE(f.AMOUNT_SELL_OUT, 0)) AS revenue_sell_out,
    SUM(COALESCE(f.VOL_SELL_OUT, 0))    AS volume_sell_out,
    SUM(COALESCE(f.PCS_INV, 0))         AS pcs_inv,
    COUNT(*)                            AS fact_row_count,

    CURRENT_TIMESTAMP() AS std_created_at

FROM PRD_MDP.MDP_DSP.VW_FACT_SELL_OUT f
LEFT JOIN PRD_MDP.MDP_DSP.VW_D_STORE_RM s
    ON TO_VARCHAR(f.STORE) = TO_VARCHAR(s.INT_ID)

WHERE f.PER_ID >= 20250101

GROUP BY
    TO_VARCHAR(f.INT_ID),
    TO_VARCHAR(f.STORE),
    s.CHAIN,
    s.FORMAT,
    LEFT(TO_VARCHAR(f.PER_ID), 6)
"""

df_so_agg = run_sf(DB_PRD_MDP, sql_so_agg)
df_so_agg.cache()
n_so_agg = df_so_agg.count()
log("INFO", f"Aggregated SELL_OUT from Snowflake: {n_so_agg:,} rows "
    f"(aggregated from 200M+ source rows)", _S)
register_join("silver_sell_out", "VW_FACT_SELL_OUT", "VW_D_STORE_RM",
              "STORE=INT_ID (pushdown in Snowflake)", "left")

# COMMAND ----------

# =============================================================================
# PHASE D — Join aggregated fact to UPC bridge (Spark — both small)
# =============================================================================
log("INFO", "Phase D: Joining aggregated fact to UPC bridge (Spark — small DataFrames)", _S)

# Build bridge lookup: sell_out_int_id → mat_idt
_common = ["sell_out_int_id", "sell_out_import_id", "so_name", "so_brand",
           "CBU_ID", "mat_idt", "sku_ean_cod",
           "match_priority", "match_method", "match_confidence", "review_status"]

def _align(df):
    for c in _common:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast("string"))
    return df.select(_common)

from functools import reduce as _reduce
df_bridge = _reduce(lambda a, b: a.union(b),
                    [_align(df_p1), _align(df_p2), _align(df_unmatched)])

# Join aggregated fact to bridge
df_so = df_so_agg.join(
    df_bridge.select("sell_out_int_id", "mat_idt", "sku_ean_cod",
                     "match_priority", "match_method", "review_status"),
    on="sell_out_int_id",
    how="left")
register_join("silver_sell_out", "sell_out_agg", "upc_bridge",
              "sell_out_int_id", "left")
assert_row_count_exact(df_so_agg, df_so,
                       "SELL_OUT aggregated × UPC bridge", _S)

# Apply M3: chain → cadena_std
df_chain_map = load_mapping_csv(
    "logs/signoff_05_store_chain_classification.csv",
    key_col="chain_value", section=_S)
df_so = df_so.join(
    df_chain_map.select(
        F.col("chain_value"),
        F.col("cadena_std").alias("cadena_std_mapped"),
        F.col("mapping_status").alias("cadena_status")),
    df_so["chain"] == df_chain_map["chain_value"], "left")
register_join("silver_sell_out", "sell_out", "chain_classification",
              "chain=chain_value", "left")
assert_row_count_exact(df_so_agg, df_so, "SELL_OUT × chain_classification", _S)

df_so = df_so.withColumn(
    "cadena_std",
    F.when(F.col("cadena_status") == "CONFIRMED", F.col("cadena_std_mapped"))
     .otherwise(F.lit(None).cast("string")))

# Apply M4: format → canal_std
df_format_map = load_mapping_csv(
    "logs/signoff_05_store_format_classification.csv",
    key_col="format_value", section=_S)
df_so = df_so.join(
    df_format_map.select(
        F.col("format_value"),
        F.col("canal_std").alias("canal_std_mapped"),
        F.col("mapping_status").alias("canal_status")),
    df_so["format"] == df_format_map["format_value"], "left")
register_join("silver_sell_out", "sell_out", "format_classification",
              "format=format_value", "left")
assert_row_count_exact(df_so_agg, df_so, "SELL_OUT × format_classification", _S)

df_so = df_so.withColumn(
    "canal_std",
    F.when(F.col("canal_status") == "CONFIRMED", F.col("canal_std_mapped"))
     .otherwise(F.lit(None).cast("string")))

# Quarantine M3/M4 NEEDS_REVIEW
quarantine(df_so.filter(F.col("cadena_std").isNull()),
           "SELL_OUT_CADENA_NEEDS_REVIEW",
           "M3 PENDING: CHAIN not yet mapped to cadena_std", _S)
quarantine(df_so.filter(F.col("canal_std").isNull()),
           "SELL_OUT_CANAL_NEEDS_REVIEW",
           "M4 PENDING: FORMAT not yet mapped to canal_std", _S)

df_so = df_so.withColumn("source_system", F.lit("SELL_OUT"))

# COMMAND ----------

# =============================================================================
# STEP 5 — Null rate audit + save
# =============================================================================
n_so = df_so.count()
log("INFO", f"sell_out_std final: {n_so:,} rows", _S)

for col_name in ["mat_idt", "cadena_std", "canal_std", "revenue_sell_out"]:
    if col_name in [c.lower() for c in df_so.columns]:
        n_null = df_so.filter(F.col(col_name).isNull()).count()
        pct = round(n_null / n_so * 100, 2) if n_so > 0 else 0.0
        log("INFO", f"NULL rate — {col_name}: {n_null:,} / {n_so:,} = {pct}%", _S)

save_df(df_so, "sell_out_std.csv", _S)
log("INFO", "sell_out_std saved. SELL_OUT standardization complete.", _S)
flush_log("phase3_standardization_audit_log.txt")

# COMMAND ----------

