# Databricks notebook source
# =============================================================================
# PHASE 3 — MKT_OFF STANDARDIZATION: silver_mkt_off_std.py
# =============================================================================
# SOURCES:  Same 4 AGG_DATA_PVT fact views (MKT_OFF = Nielsen MKT off-take)
# OUTPUT:   mkt_off_std
#
# CRITICAL RULES:
#   R7:  MKT_OFF NEVER joins by UPC
#   R8:  MKT_OFF NEVER joins by CADENA
#   R9:  cadena_std IS INTENTIONALLY NULL — approved structural hardcoding.
#        NULL::VARCHAR AS cadena_std is an architectural contract (NOT a R12 violation).
#        R12 prohibits mapping logic in notebooks; a guaranteed-NULL by design is not a
#        mapping rule. See Phase 3 plan v2 Change #10.
#   R13: Join to nielsen_std (unique) — never to raw MKT_DIM
#   R14: Uniqueness assertion on every mapping before join
#   R15: assert_row_count_exact after every join
#
# STRUCTURAL ASSERTIONS (A2, A3, A4):
#   A2: assert_no_prohibited_join UPC keys in MKT_OFF JOIN_REGISTRY entries
#   A3: assert_no_prohibited_join CADENA keys in MKT_OFF JOIN_REGISTRY entries
#   A4: cadena_std IS NULL for every row in mkt_off_std
# =============================================================================

# COMMAND ----------

# MAGIC %run ./silver_homologation_apply

# COMMAND ----------

# MAGIC %md
# MAGIC ## MKT_OFF Standardization
# MAGIC
# MAGIC **Purpose**: Build `mkt_off_std` — Nielsen off-take fact data enriched with
# MAGIC `canal_std` / `region_std` / `marca_std`.
# MAGIC
# MAGIC > **IMPORTANT — R9 architectural contract**:
# MAGIC > `cadena_std` IS INTENTIONALLY NULL in `mkt_off_std`.
# MAGIC > This is an **approved architectural contract** (R9), not a data quality gap.
# MAGIC > MKT_OFF data has no chain-level granularity by design; any non-NULL `cadena_std`
# MAGIC > value in this table is a pipeline error and will be caught by assertion A4.
# MAGIC
# MAGIC **Key rules enforced**:
# MAGIC - **R7**: MKT_OFF NEVER joins by UPC, EAN, or SKU_EAN
# MAGIC - **R8**: MKT_OFF NEVER joins by CADENA
# MAGIC - **R9**: `cadena_std` is hardcoded NULL — not a mapping gap
# MAGIC - **R13**: Join to `nielsen_std` (pre-deduplicated, unique on `MRKT_DSC_SHRT`)
# MAGIC - **R14**: Uniqueness assertion on `nielsen_std` before every join
# MAGIC - **R15**: `assert_row_count_exact` after every join
# MAGIC
# MAGIC **Prerequisites**: `silver_nielsen.py` must have run first to produce `logs/nielsen_std.csv`.

# COMMAND ----------

_S = "MKT_OFF"
log("INFO", "Starting MKT_OFF standardization — loading nielsen_std", _S)

# Load pre-built nielsen_std (unique on MRKT_DSC_SHRT, R13)
# Written by silver_nielsen.py — must be run first
nielsenSTD_path = str(REPO_ROOT / "logs" / "nielsen_std.csv")
if not os.path.exists(nielsenSTD_path):
    blocker(True,
        f"nielsen_std.csv not found at {nielsenSTD_path}. "
        "Run silver_nielsen.py before silver_mkt_off_std.py.",
        _S)
else:
    df_nielsen_std = (spark.read
                      .option("header", "true")
                      .csv(f"file://{nielsenSTD_path}"))
    # Assert uniqueness before any join (R14)
    dup_ns = df_nielsen_std.count() - df_nielsen_std.dropDuplicates(["MRKT_DSC_SHRT"]).count()
    blocker(dup_ns > 0,
        f"nielsen_std has {dup_ns} duplicate MRKT_DSC_SHRT values — cannot join safely.",
        _S)
    log("INFO", f"nielsen_std loaded: {df_nielsen_std.count()} unique market strings", _S)

# COMMAND ----------

log("INFO", "Starting MKT_OFF standardization", _S)

_MKT_OFF_AGG_TABLES = {
    "EDP_NIELSEN":     "PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_AGG_DATA_PVT",
    "PB_NIELSEN":      "PRD_MEX.MEX_DSP_DPH_MKT.VW_SUST_LECHE_ST_NLSN_AGG_DATA_PVT",
    "WATER_RETAIL":    "PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_RT_NLSN_AGG_DATA_PVT",
    "WATER_SCANTRACK": "PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_ST_NLSN_AGG_DATA_PVT",
}

brand_cfg = load_yaml_config("configs/brand_crosswalk.yaml")

_mkt_off_frames = []
for cbu_label, tbl in _MKT_OFF_AGG_TABLES.items():
    try:
        df_fact = run_sf(DB_PRD_MEX, f"SELECT * FROM {tbl}")
        df_fact.cache()
        n_fact = df_fact.count()
        log("INFO", f"{cbu_label}: {n_fact:,} fact rows", _S)

        df_off = df_fact.join(
            df_nielsen_std.select(
                F.col("MRKT_DSC_SHRT"),
                F.col("canal_std"),
                F.col("region_std"),
                F.col("mapping_status").alias("market_mapping_status")),
            on="MRKT_DSC_SHRT", how="left")
        register_join("silver_mkt_off_std", f"{cbu_label}_fact", "nielsen_std",
                      "MRKT_DSC_SHRT", "left")
        assert_row_count_exact(df_fact, df_off,
                               f"MKT_OFF {cbu_label} × nielsen_std", _S)

        # R9 — cadena_std IS INTENTIONALLY NULL (approved structural hardcoding, Change #10)
        # This is not a mapping gap. MKT_OFF never has chain-level data by design.
        df_off = df_off.withColumn("cadena_std", F.lit(None).cast(StringType()))

        # marca_std from brand field
        brand_col = None
        for col in df_off.columns:
            if "BRAND" in col.upper() or "MARCA" in col.upper():
                brand_col = col
                break
        if brand_col:
            df_off = df_off.withColumn("marca_std", F.upper(F.trim(F.col(brand_col))))
        else:
            df_off = df_off.withColumn("marca_std", F.lit(None).cast("string"))
            warn(True, f"{cbu_label}: No brand column found — marca_std=NULL", _S)

        df_off = (df_off
                  .withColumn("cbu_source", F.lit(cbu_label))
                  .withColumn("source_system", F.lit("MKT_OFF"))
                  .withColumn("std_created_at", F.current_timestamp()))
        _mkt_off_frames.append(df_off)
    except Exception as e:
        warn(True, f"Cannot read MKT_OFF fact for {cbu_label}: {e}", _S)

if _mkt_off_frames:
    from functools import reduce as _reduce
    df_mkt_off_std = _reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True),
                             _mkt_off_frames)
    n_mkt_off = df_mkt_off_std.count()
    log("INFO", f"mkt_off_std: {n_mkt_off:,} total rows", _S)
    save_df(df_mkt_off_std, "mkt_off_std.csv", _S)
else:
    blocker(True, "No MKT_OFF fact tables produced output — mkt_off_std empty", _S)
    df_mkt_off_std = None
    n_mkt_off = 0

# COMMAND ----------

# A4: MKT_OFF cadena_std must be NULL for every single row (R9)
# This is the key structural correctness check for MKT_OFF.
if df_mkt_off_std is not None:
    n_non_null_cadena = df_mkt_off_std.filter(F.col("cadena_std").isNotNull()).count()
    blocker(n_non_null_cadena > 0,
        f"A4 VIOLATION: {n_non_null_cadena:,} rows in mkt_off_std have non-NULL cadena_std. "
        "cadena_std must be NULL in MKT_OFF by design (R9). "
        "A cadena value was accidentally populated — investigate the join logic.",
        _S)
    if n_non_null_cadena == 0:
        passed(f"A4: cadena_std is NULL for all {n_mkt_off:,} rows in mkt_off_std (R9 confirmed)", _S)

# COMMAND ----------

# Filter JOIN_REGISTRY to MKT_OFF entries only before scanning
mkt_off_entries = [e for e in JOIN_REGISTRY if e["notebook"] == "silver_mkt_off_std"]
old_registry = JOIN_REGISTRY.copy()
JOIN_REGISTRY.clear()
JOIN_REGISTRY.extend(mkt_off_entries)

# A2: R7 — MKT_OFF must never join by UPC
assert_no_prohibited_join(
    prohibited_keys=["UPC", "EAN", "SKU_EAN", "SKU_EAN_COD", "INT_ID"],
    rule_label="R7 — MKT_OFF must never join by UPC",
    section=_S)

# A3: R8 — MKT_OFF must never join by CADENA
assert_no_prohibited_join(
    prohibited_keys=["CADENA", "cadena_std", "CUS_CADENA"],
    rule_label="R8 — MKT_OFF must never join by CADENA",
    section=_S)

# Restore full registry
JOIN_REGISTRY.clear()
JOIN_REGISTRY.extend(old_registry)

flush_log("phase3_standardization_audit_log.txt")
log("INFO", "MKT_OFF standardization complete.", _S)
