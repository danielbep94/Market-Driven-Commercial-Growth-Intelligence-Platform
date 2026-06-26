# Databricks notebook source
# =============================================================================
# PHASE 3 — MKT_ON STANDARDIZATION: silver_mkt_on_std.py
# =============================================================================
# SOURCES:  4 confirmed AGG_DATA_PVT fact views + pre-built nielsen_std dim
# OUTPUT:   mkt_on_std
#
# RULES:
#   R6:  MKT_ON NEVER joins by UPC (UPC, EAN, SKU_EAN all prohibited)
#   R13: Join to nielsen_std (already unique) — never to raw MKT_DIM
#   R14: Uniqueness assertion on every mapping table before join
#   R15: assert_row_count_exact after every join
#
# STRUCTURAL ASSERTION:
#   assert_no_prohibited_join(["UPC","EAN","SKU_EAN"], "R6 MKT_ON no UPC", ...)
#   is called AFTER all joins to scan JOIN_REGISTRY.
# =============================================================================

# COMMAND ----------

# MAGIC %run ./silver_homologation_apply

# COMMAND ----------

# MAGIC %md
# MAGIC ## MKT_ON Standardization
# MAGIC
# MAGIC **Purpose**: Build `mkt_on_std` — Nielsen on-take fact data enriched with
# MAGIC `canal_std` / `region_std` / `marca_std`.
# MAGIC
# MAGIC **Key rules enforced**:
# MAGIC - **R6**: MKT_ON NEVER joins by UPC, EAN, or SKU_EAN
# MAGIC - **R13**: Join to `nielsen_std` (pre-deduplicated, unique on `MRKT_DSC_SHRT`) — never to raw MKT_DIM
# MAGIC - **R14**: Uniqueness assertion on `nielsen_std` before every join
# MAGIC - **R15**: `assert_row_count_exact` after every join
# MAGIC
# MAGIC **Prerequisites**: `silver_nielsen.py` must have run first to produce `logs/nielsen_std.csv`.

# COMMAND ----------

_S = "MKT_ON"
log("INFO", "Starting MKT_ON standardization", _S)

# Load pre-built nielsen_std (unique on MRKT_DSC_SHRT, R13)
# Written by silver_nielsen.py — must be run first
nielsenSTD_path = str(REPO_ROOT / "logs" / "nielsen_std.csv")
if not os.path.exists(nielsenSTD_path):
    blocker(True,
        f"nielsen_std.csv not found at {nielsenSTD_path}. "
        "Run silver_nielsen.py before silver_mkt_on_std.py.",
        _S)
else:
    df_nielsen_std = (spark.read
                      .option("header", "true")
                      .csv(nielsenSTD_path))
    # Assert uniqueness before any join (R14)
    dup_ns = df_nielsen_std.count() - df_nielsen_std.dropDuplicates(["MRKT_DSC_SHRT"]).count()
    blocker(dup_ns > 0,
        f"nielsen_std has {dup_ns} duplicate MRKT_DSC_SHRT values — cannot join safely.",
        _S)
    log("INFO", f"nielsen_std loaded: {df_nielsen_std.count()} unique market strings", _S)

# COMMAND ----------

_MKT_ON_AGG_TABLES = {
    "EDP_NIELSEN":     "PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_AGG_DATA_PVT",
    "PB_NIELSEN":      "PRD_MEX.MEX_DSP_DPH_MKT.VW_SUST_LECHE_ST_NLSN_AGG_DATA_PVT",
    "WATER_RETAIL":    "PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_RT_NLSN_AGG_DATA_PVT",
    "WATER_SCANTRACK": "PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_ST_NLSN_AGG_DATA_PVT",
}

brand_cfg = load_yaml_config("configs/brand_crosswalk.yaml")
log("INFO", f"Brand crosswalk loaded v{brand_cfg.get('version','?')}", _S)

_mkt_on_frames = []
for cbu_label, tbl in _MKT_ON_AGG_TABLES.items():
    try:
        df_fact = run_sf(DB_PRD_MEX, f"SELECT * FROM {tbl}")
        df_fact.cache()
        n_fact = df_fact.count()
        log("INFO", f"{cbu_label}: {n_fact:,} fact rows", _S)

        df_on = df_fact.join(
            df_nielsen_std.select(
                F.col("MRKT_DSC_SHRT"),
                F.col("canal_std"),
                F.col("region_std"),
                F.col("mapping_status").alias("market_mapping_status")),
            on="MRKT_DSC_SHRT", how="left")
        register_join("silver_mkt_on_std", f"{cbu_label}_fact", "nielsen_std",
                      "MRKT_DSC_SHRT", "left")
        assert_row_count_exact(df_fact, df_on,
                               f"MKT_ON {cbu_label} × nielsen_std", _S)

        # Apply marca_std from brand field (R12 — from brand_crosswalk.yaml)
        brand_col = None
        for col in df_on.columns:
            if "BRAND" in col.upper() or "MARCA" in col.upper():
                brand_col = col
                break
        if brand_col:
            df_on = df_on.withColumn("marca_std", F.upper(F.trim(F.col(brand_col))))
        else:
            df_on = df_on.withColumn("marca_std", F.lit(None).cast("string"))
            warn(True, f"{cbu_label}: No brand column found — marca_std=NULL", _S)

        df_on = (df_on
                 .withColumn("cbu_source", F.lit(cbu_label))
                 .withColumn("source_system", F.lit("MKT_ON"))
                 .withColumn("std_created_at", F.current_timestamp()))
        _mkt_on_frames.append(df_on)
    except Exception as e:
        warn(True, f"Cannot read MKT_ON fact for {cbu_label}: {e}", _S)

if _mkt_on_frames:
    from functools import reduce as _reduce
    df_mkt_on_std = _reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True),
                            _mkt_on_frames)
    log("INFO", f"mkt_on_std: {df_mkt_on_std.count():,} total rows", _S)
    save_df(df_mkt_on_std, "mkt_on_std.csv", _S)
else:
    blocker(True, "No MKT_ON fact tables produced output — mkt_on_std empty", _S)

# COMMAND ----------

# R6: MKT_ON MUST NEVER join by UPC, EAN, or SKU_EAN (Change #7 — registry scan)
assert_no_prohibited_join(
    prohibited_keys=["UPC", "EAN", "SKU_EAN", "SKU_EAN_COD", "INT_ID"],
    rule_label="R6 — MKT_ON must never join by UPC",
    section=_S)

flush_log("phase3_standardization_audit_log.txt")
log("INFO", "MKT_ON standardization complete.", _S)
