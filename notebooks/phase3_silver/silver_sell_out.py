# Databricks notebook source
# =============================================================================
# PHASE 3 — SELL_OUT STANDARDIZATION: silver_sell_out.py
# =============================================================================
# SOURCE FACT:  PRD_MDP.MDP_DSP.VW_FACT_SELL_OUT
# DIMENSIONS:   VW_D_PRODUCT_RM (product), VW_D_STORE_RM (store)
# OUTPUT:       sell_out_std
# GRAIN:        one row per fact record in VW_FACT_SELL_OUT
#
# RULES ENFORCED:
#   R5:  UPC_STD is bridge-only — not a join predicate in the all-source join
#   R10: P1 bridge dedup = MIN(MAT_IDT) GROUP BY EAN — guaranteed 1:1
#   R11: Fuzzy matches quarantine-only
#   R14: load_mapping_csv uniqueness assertion before every join
#   R15: assert_row_count_exact after every left join
#
# BRIDGE TIERS:
#   P1: INT_ID = SKU_EAN_COD   (match_priority=1, EXACT_INT_ID)
#   P2: IMPORT_ID = SKU_EAN_COD (match_priority=2, EXACT_IMPORT_ID)
#   P3: UNMATCHED               (match_priority=3, quarantine)
# =============================================================================

# COMMAND ----------

# MAGIC %run ./silver_homologation_apply

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 3 — SELL_OUT Standardization
# MAGIC **Source:** `PRD_MDP.MDP_DSP.VW_FACT_SELL_OUT`
# MAGIC **Dimensions:** `VW_D_PRODUCT_RM` (product), `VW_D_STORE_RM` (store)
# MAGIC **Output:** `sell_out_std`
# MAGIC **Grain:** one row per fact record in `VW_FACT_SELL_OUT`

# COMMAND ----------

_S = "SELL_OUT"
log("INFO", "Starting SELL_OUT standardization", _S)

df_fact_so = run_sf(DB_PRD_MDP, "SELECT * FROM PRD_MDP.MDP_DSP.VW_FACT_SELL_OUT")
df_fact_so.cache()
n_fact_so = df_fact_so.count()
log("INFO", f"VW_FACT_SELL_OUT: {n_fact_so:,} rows", _S)

df_product = run_sf(DB_PRD_MDP, """
    SELECT TO_VARCHAR(INT_ID) AS sell_out_int_id,
           TO_VARCHAR(IMPORT_ID) AS sell_out_import_id,
           NAME AS so_name, BRAND AS so_brand, CBU_ID
    FROM PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM
""")
df_product.cache()
n_product = df_product.count()
log("INFO", f"VW_D_PRODUCT_RM: {n_product:,} rows", _S)

df_store = run_sf(DB_PRD_MDP, "SELECT * FROM PRD_MDP.MDP_DSP.VW_D_STORE_RM")
df_store.cache()
log("INFO", f"VW_D_STORE_RM: {df_store.count():,} rows", _S)

# COMMAND ----------

# Dedup V_D_ITEM to one MAT_IDT per EAN — MIN(MAT_IDT) GROUP BY EAN (R10)
# MAT_ACT_FLG is NULL for all rows (confirmed Phase 2) — no active filter
sql_ean_dedup = """
    SELECT
        TO_VARCHAR(SKU_EAN_COD)  AS sku_ean_cod,
        MIN(TO_VARCHAR(MAT_IDT)) AS mat_idt,
        MIN(MAT_LCL_DSC)         AS si_description
    FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM
    WHERE SKU_EAN_COD IS NOT NULL
      AND TRIM(TO_VARCHAR(SKU_EAN_COD)) <> ''
    GROUP BY TO_VARCHAR(SKU_EAN_COD)
"""
df_ean_dedup = run_sf(DB_PRD_MEX, sql_ean_dedup)
df_ean_dedup.cache()
n_ean = df_ean_dedup.count()
log("INFO", f"EAN dedup table: {n_ean:,} unique EANs (one MAT_IDT per EAN, R10)", _S)

# Assert uniqueness of deduped EAN table before ANY join (R14 — Change #5)
dup_ean = df_ean_dedup.count() - df_ean_dedup.dropDuplicates(["sku_ean_cod"]).count()
blocker(dup_ean > 0,
    f"EAN dedup table has {dup_ean} duplicate sku_ean_cod values — P1 join would fanout. "
    "Check MIN(MAT_IDT) GROUP BY logic.",
    _S)

# COMMAND ----------

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

p1_matched_int_ids = set(
    r["sell_out_int_id"]
    for r in df_p1.select("sell_out_int_id").distinct().collect())
n_p1 = df_p1.count()
log("INFO",
    f"P1 matches: {n_p1:,} rows | {len(p1_matched_int_ids):,} distinct INT_IDs matched",
    _S)

# Assert P1 bridge is 1:1 per EAN — no fanout allowed (A5, A6)
df_p1_fanout = (df_p1.groupBy("sku_ean_cod")
                .agg(F.countDistinct("mat_idt").alias("distinct_mat_idt"))
                .filter(F.col("distinct_mat_idt") > 1))
n_fanout = df_p1_fanout.count()
blocker(n_fanout > 0,
    f"P1 bridge has {n_fanout:,} EANs mapping to >1 distinct MAT_IDT. "
    "EAN dedup did not produce a 1:1 bridge. Investigate V_D_ITEM.",
    _S)
if n_fanout == 0:
    passed("P1 bridge: 0 ambiguous EANs — 1:1 guarantee confirmed (A5, A6)", _S)

# COMMAND ----------

# Exclude products already matched in P1
df_p2_candidates = df_product.filter(
    ~F.col("sell_out_int_id").isin(p1_matched_int_ids))

df_p2 = (df_p2_candidates
         .join(df_ean_dedup,
               df_p2_candidates["sell_out_import_id"] == df_ean_dedup["sku_ean_cod"],
               "inner")
         .withColumn("match_priority",   F.lit(2))
         .withColumn("match_method",     F.lit("EXACT_IMPORT_ID"))
         .withColumn("match_confidence", F.lit(0.9))
         .withColumn("review_status",    F.lit("CONFIRMED")))
register_join("silver_sell_out", "VW_D_PRODUCT_RM_p2_candidates",
              "V_D_ITEM_ean_dedup", "sell_out_import_id=sku_ean_cod", "inner")
n_p2 = df_p2.count()
log("INFO", f"P2 matches: {n_p2:,} rows via IMPORT_ID", _S)

# COMMAND ----------

p2_matched_import_ids = set(
    r["sell_out_import_id"]
    for r in df_p2.select("sell_out_import_id").distinct().collect())

df_unmatched = (df_product
                .filter(~F.col("sell_out_int_id").isin(p1_matched_int_ids))
                .filter(
                    ~F.col("sell_out_import_id").isin(p2_matched_import_ids)
                    if p2_matched_import_ids
                    else F.lit(True))
                .withColumn("match_priority",   F.lit(3))
                .withColumn("match_method",     F.lit("UNMATCHED"))
                .withColumn("match_confidence", F.lit(0.0))
                .withColumn("review_status",    F.lit("NEEDS_REVIEW"))
                .withColumn("mat_idt",           F.lit(None).cast("string"))
                .withColumn("sku_ean_cod",       F.lit(None).cast("string")))

n_unmatched = df_unmatched.count()
quarantine(df_unmatched, "SELL_OUT_P3_UNMATCHED",
           "No EAN match via INT_ID or IMPORT_ID", _S)

# Cascade summary
total_products = n_product
p1_pct = round(n_p1 / total_products * 100, 2) if total_products > 0 else 0.0
p2_pct = round(n_p2 / total_products * 100, 2) if total_products > 0 else 0.0
um_pct = round(n_unmatched / total_products * 100, 2) if total_products > 0 else 0.0
log("INFO",
    f"UPC Cascade: P1={n_p1:,} ({p1_pct}%) | P2={n_p2:,} ({p2_pct}%) "
    f"| Unmatched={n_unmatched:,} ({um_pct}%)",
    _S)
warn(p1_pct < 70.0,
     f"P1 match rate {p1_pct}% is below 70% threshold. "
     "Investigate SELL_OUT product catalog EAN coverage.",
     _S)
if p1_pct >= 70.0:
    passed(f"P1 match rate {p1_pct}% >= 70% threshold", _S)

# COMMAND ----------

from functools import reduce as _reduce

common_cols = ["sell_out_int_id", "sell_out_import_id", "so_name", "so_brand",
               "CBU_ID", "mat_idt", "sku_ean_cod",
               "match_priority", "match_method", "match_confidence", "review_status"]

def _align(df):
    for c in common_cols:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast("string"))
    return df.select(common_cols)

df_bridge = _reduce(lambda a, b: a.union(b),
                    [_align(df_p1), _align(df_p2), _align(df_unmatched)])

# Join fact to bridge on product key present in VW_FACT_SELL_OUT
fact_cols_upper = [c.upper() for c in df_fact_so.columns]
if "INT_ID" in fact_cols_upper:
    FACT_SO_PROD_KEY = "INT_ID"
elif "PRODUCT_ID" in fact_cols_upper:
    FACT_SO_PROD_KEY = "PRODUCT_ID"
else:
    warn(True,
         f"No known product key in VW_FACT_SELL_OUT: {df_fact_so.columns}. "
         "Joining on literal NULL — sell_in bridge will not link to fact.",
         _S)
    FACT_SO_PROD_KEY = None

if FACT_SO_PROD_KEY:
    df_so = df_fact_so.join(
        df_bridge,
        F.col(FACT_SO_PROD_KEY).cast("string") == F.col("sell_out_int_id"),
        "left")
    register_join("silver_sell_out", "VW_FACT_SELL_OUT", "upc_bridge",
                  f"{FACT_SO_PROD_KEY}=sell_out_int_id", "left")
    assert_row_count_exact(df_fact_so, df_so, "SELL_OUT fact × UPC bridge", _S)
else:
    df_so = df_fact_so

# COMMAND ----------

# Load M3: chain → cadena_std (19 chains, R14)
df_chain_map = load_mapping_csv(
    "logs/signoff_05_store_chain_classification.csv",
    key_col="chain_value", section=_S)

# Load M4: format → canal_std (86 formats, R14)
df_format_map = load_mapping_csv(
    "logs/signoff_05_store_format_classification.csv",
    key_col="format_value", section=_S)

# Detect store join key in fact
if "STORE" in fact_cols_upper:
    FACT_STORE_KEY = "STORE"
elif "STORE_ID" in fact_cols_upper:
    FACT_STORE_KEY = "STORE_ID"
else:
    warn(True,
         f"No store key (STORE/STORE_ID) in VW_FACT_SELL_OUT: {df_fact_so.columns}. "
         "Store dim join skipped.",
         _S)
    FACT_STORE_KEY = None

if FACT_STORE_KEY:
    store_key_in_store = "INT_ID" if "INT_ID" in [c.upper() for c in df_store.columns] else None
    if store_key_in_store:
        df_so = df_so.join(
            df_store,
            F.col(FACT_STORE_KEY).cast("string") == df_store[store_key_in_store].cast("string"),
            "left")
        register_join("silver_sell_out", "sell_out", "VW_D_STORE_RM",
                      f"{FACT_STORE_KEY}={store_key_in_store}", "left")
        assert_row_count_exact(df_fact_so, df_so, "SELL_OUT × VW_D_STORE_RM", _S)

    # Apply M3: CHAIN → cadena_std
    if "CHAIN" in [c.upper() for c in df_so.columns]:
        df_so = df_so.join(
            df_chain_map.select(
                F.col("chain_value"),
                F.col("cadena_std").alias("cadena_std_mapped"),
                F.col("mapping_status").alias("cadena_mapping_status")),
            df_so["CHAIN"] == df_chain_map["chain_value"], "left")
        register_join("silver_sell_out", "sell_out", "chain_classification",
                      "CHAIN=chain_value", "left")
        assert_row_count_exact(df_fact_so, df_so, "SELL_OUT × chain_classification", _S)

        df_so = df_so.withColumn(
            "cadena_std",
            F.when(F.col("cadena_mapping_status") == "CONFIRMED",
                   F.col("cadena_std_mapped"))
             .otherwise(F.lit(None).cast("string")))

        # Quarantine NEEDS_REVIEW cadena rows (M3 pending)
        df_cadena_nr = df_so.filter(F.col("cadena_std").isNull())
        quarantine(df_cadena_nr, "SELL_OUT_CADENA_NEEDS_REVIEW",
                   "M3 PENDING: CHAIN not yet mapped to cadena_std", _S)
    else:
        df_so = df_so.withColumn("cadena_std", F.lit(None).cast("string"))
        warn(True, "CHAIN column not found in joined DataFrame — cadena_std=NULL", _S)

    # Apply M4: FORMAT → canal_std
    if "FORMAT" in [c.upper() for c in df_so.columns]:
        df_so = df_so.join(
            df_format_map.select(
                F.col("format_value"),
                F.col("canal_std").alias("canal_std_mapped"),
                F.col("mapping_status").alias("canal_mapping_status")),
            df_so["FORMAT"] == df_format_map["format_value"], "left")
        register_join("silver_sell_out", "sell_out", "format_classification",
                      "FORMAT=format_value", "left")
        assert_row_count_exact(df_fact_so, df_so, "SELL_OUT × format_classification", _S)

        df_so = df_so.withColumn(
            "canal_std",
            F.when(F.col("canal_mapping_status") == "CONFIRMED",
                   F.col("canal_std_mapped"))
             .otherwise(F.lit(None).cast("string")))

        df_canal_nr = df_so.filter(F.col("canal_std").isNull())
        quarantine(df_canal_nr, "SELL_OUT_CANAL_NEEDS_REVIEW",
                   "M4 PENDING: FORMAT not yet mapped to canal_std", _S)
    else:
        df_so = df_so.withColumn("canal_std", F.lit(None).cast("string"))
        warn(True, "FORMAT column not found in joined DataFrame — canal_std=NULL", _S)
else:
    df_so = df_so.withColumn("cadena_std", F.lit(None).cast("string"))
    df_so = df_so.withColumn("canal_std",  F.lit(None).cast("string"))

df_so = df_so.withColumn("source_system", F.lit("SELL_OUT"))
df_so = df_so.withColumn("std_created_at", F.current_timestamp())

# COMMAND ----------

n_so = df_so.count()
log("INFO", f"sell_out_std rows: {n_so:,} (source VW_FACT_SELL_OUT: {n_fact_so:,})", _S)

for col_name in ["mat_idt", "cadena_std", "canal_std"]:
    if col_name in [c.lower() for c in df_so.columns]:
        n_null = df_so.filter(F.col(col_name).isNull()).count()
        pct = round(n_null / n_so * 100, 2) if n_so > 0 else 0.0
        log("INFO", f"NULL rate — {col_name}: {n_null:,} / {n_so:,} = {pct}%", _S)

save_df(df_so, "sell_out_std.csv", _S)
log("INFO", "sell_out_std saved. SELL_OUT standardization complete.", _S)
flush_log("phase3_standardization_audit_log.txt")
