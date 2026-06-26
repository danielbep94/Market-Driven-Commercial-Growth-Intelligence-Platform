# Databricks notebook source
# =============================================================================
# PHASE 3 — NIELSEN STANDARDIZATION: silver_nielsen.py
# =============================================================================
# SOURCES:  4 MKT_DIM views + 4 AGG_DATA_PVT fact views in PRD_MEX.MEX_DSP_DPH_MKT
# OUTPUT:   nielsen_std (unique market dimension) + nielsen_facts_std (fact enriched)
#
# KEY DESIGN (Change #3 — R13):
#   Step A: Build deduplicated market dimension FIRST (unique on mrkt_dsc_shrt)
#   Step B: Apply M1 mapping (canal_std / region_std)
#   Step C: Join facts to the unique nielsen_std dim (separate from dim build)
#   This 3-step order prevents fanout when mrkt_dsc_shrt repeats across CBUs.
#
# MAPPING SOURCE (Change #8):
#   M1 = logs/signoff_03_nielsen_markets.csv (362 rows — authoritative)
#   NOT homologation/nielsen_market_mapping.csv (11 rows — partial predecessor)
#
# RULES:
#   R6:  MKT_ON never joins by UPC
#   R7:  MKT_OFF never joins by UPC
#   R13: nielsen_std is unique on mrkt_dsc_shrt before any fact join
#   R14: load_mapping_csv uniqueness assertion before every join
#   R15: assert_row_count_exact after every left join
# =============================================================================

# COMMAND ----------

# MAGIC %run ./silver_homologation_apply

# COMMAND ----------

# MAGIC %md
# MAGIC ## NIELSEN Standardization — 3-Step Design
# MAGIC
# MAGIC **Purpose**: Build `nielsen_std` (unique market dimension) and `nielsen_facts_std` (fact-enriched).
# MAGIC
# MAGIC **3-Step order (Change #3 — R13)**:
# MAGIC - **Step A**: Union 4 MKT_DIM views → deduplicate to 1 row per `MRKT_DSC_SHRT`
# MAGIC - **Step B**: Apply M1 mapping (`canal_std` / `region_std`) from `signoff_03_nielsen_markets.csv`
# MAGIC - **Step C**: Join 4 AGG_DATA_PVT fact views to the already-unique `nielsen_std` dim
# MAGIC
# MAGIC This order prevents fanout: the dim is guaranteed unique before any fact join (R13).

# COMMAND ----------

_S = "NIELSEN"
log("INFO", "Starting NIELSEN standardization — Step A: Build unique market dimension", _S)

# Confirmed exact table names from pipeline_config.yaml (Change #6 — no wildcard)
_MKT_DIM_TABLES = {
    "EDP_NIELSEN":       "PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_MKT_DIM",
    "PB_NIELSEN":        "PRD_MEX.MEX_DSP_DPH_MKT.VW_SUST_LECHE_ST_NLSN_MKT_DIM",
    "WATER_RETAIL":      "PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_RT_NLSN_MKT_DIM",
    "WATER_SCANTRACK":   "PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_ST_NLSN_MKT_DIM",
}

_mkt_frames = []
for cbu_label, tbl in _MKT_DIM_TABLES.items():
    try:
        df_mkt = run_sf(DB_PRD_MEX, f"SELECT DISTINCT MRKT_DSC_SHRT FROM {tbl} WHERE MRKT_DSC_SHRT IS NOT NULL")
        df_mkt = df_mkt.withColumn("nielsen_source_cbu", F.lit(cbu_label))
        n = df_mkt.count()
        log("INFO", f"{cbu_label}: {n} distinct MRKT_DSC_SHRT values", _S)
        _mkt_frames.append(df_mkt)
    except Exception as e:
        blocker(True, f"Cannot read MKT_DIM for {cbu_label} ({tbl}): {e}", _S)

if not _mkt_frames:
    blocker(True, "All 4 MKT_DIM tables failed — cannot build nielsen_std", _S)

from functools import reduce as _reduce
df_mkt_raw = _reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), _mkt_frames)
n_raw = df_mkt_raw.count()
log("INFO", f"Raw union: {n_raw} rows across {len(_mkt_frames)} CBU tables", _S)

# R13 — Change #3: deduplicate to 1 row per mrkt_dsc_shrt
df_nielsen_dim = df_mkt_raw.dropDuplicates(["MRKT_DSC_SHRT"])
n_dim = df_nielsen_dim.count()
log("INFO",
    f"Nielsen market dim after dedup: {n_dim} unique MRKT_DSC_SHRT values "
    f"({n_raw - n_dim} cross-CBU duplicates removed, R13)",
    _S)

# Assert uniqueness of the dim before ANY join (R14)
dup_mkt = df_nielsen_dim.count() - df_nielsen_dim.dropDuplicates(["MRKT_DSC_SHRT"]).count()
blocker(dup_mkt > 0,
    f"Nielsen market dim still has {dup_mkt} duplicate MRKT_DSC_SHRT values after dedup — investigate.",
    _S)

# COMMAND ----------

log("INFO", "Step B: Applying M1 market mapping (canal_std / region_std)", _S)

# M1 AUTHORITATIVE SOURCE: logs/signoff_03_nielsen_markets.csv (362 rows)
# NOT homologation/nielsen_market_mapping.csv (11 rows — partial predecessor) (Change #8)
df_m1 = load_mapping_csv(
    "logs/signoff_03_nielsen_markets.csv",
    key_col="mrkt_dsc_shrt", section=_S)

# Join dim to M1
df_nielsen_std = df_nielsen_dim.join(
    df_m1.select(
        F.col("mrkt_dsc_shrt").alias("m1_mrkt_dsc_shrt"),
        F.lit(None).cast("string").alias("canal_std"),   # M1 PENDING: not yet in signoff CSV
        F.lit(None).cast("string").alias("region_std"),  # M1 PENDING: not yet in signoff CSV
        F.col("REVIEW_STATUS").alias("mapping_status")),
    df_nielsen_dim["MRKT_DSC_SHRT"] == F.col("m1_mrkt_dsc_shrt"),
    "left"
)
register_join("silver_nielsen", "nielsen_market_dim", "signoff_03_nielsen_markets",
              "MRKT_DSC_SHRT=mrkt_dsc_shrt", "left")
assert_row_count_exact(df_nielsen_dim, df_nielsen_std,
                       "nielsen_std × M1 market mapping", _S)

# Quarantine NEEDS_REVIEW market strings
df_nr = df_nielsen_std.filter(
    F.col("mapping_status").isNull() | (F.col("mapping_status") == "NEEDS_REVIEW"))
quarantine(df_nr, "NIELSEN_MARKET_NEEDS_REVIEW",
           "M1 PENDING: market string not yet mapped to canal_std/region_std", _S)

n_confirmed = df_nielsen_std.filter(F.col("mapping_status") == "CONFIRMED").count()
n_pending   = df_nr.count()
log("INFO",
    f"M1 mapping: {n_confirmed} CONFIRMED | {n_pending} NEEDS_REVIEW "
    f"(coverage: {round(n_confirmed/n_dim*100, 1) if n_dim > 0 else 0}%)",
    _S)
warn(n_confirmed / n_dim < 0.50 if n_dim > 0 else True,
     f"Nielsen canal_std/region_std coverage below 50% threshold. Complete M1.", _S)

save_df(df_nielsen_std, "nielsen_std.csv", _S)
log("INFO", f"nielsen_std saved: {n_dim} unique market strings", _S)

# COMMAND ----------

log("INFO", "Step C: Enriching Nielsen fact tables with nielsen_std", _S)

# Confirmed exact fact table names from pipeline_config.yaml (Change #6)
_AGG_TABLES = {
    "EDP_NIELSEN":     "PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_AGG_DATA_PVT",
    "PB_NIELSEN":      "PRD_MEX.MEX_DSP_DPH_MKT.VW_SUST_LECHE_ST_NLSN_AGG_DATA_PVT",
    "WATER_RETAIL":    "PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_RT_NLSN_AGG_DATA_PVT",
    "WATER_SCANTRACK": "PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_ST_NLSN_AGG_DATA_PVT",
}

_fact_frames_std = []
for cbu_label, tbl in _AGG_TABLES.items():
    try:
        df_fact = run_sf(DB_PRD_MEX, f"SELECT * FROM {tbl}")
        df_fact.cache()
        n_fact = df_fact.count()
        log("INFO", f"{cbu_label} fact: {n_fact:,} rows", _S)

        # Join to nielsen_std (already unique on MRKT_DSC_SHRT — guaranteed 1:1, R13)
        df_enriched = df_fact.join(
            df_nielsen_std.select(
                F.col("MRKT_DSC_SHRT"),
                F.col("canal_std"),
                F.col("region_std"),
                F.col("mapping_status").alias("market_mapping_status")),
            on="MRKT_DSC_SHRT",
            how="left"
        )
        register_join("silver_nielsen", f"{cbu_label}_fact", "nielsen_std",
                      "MRKT_DSC_SHRT", "left")
        assert_row_count_exact(df_fact, df_enriched,
                               f"{cbu_label} fact × nielsen_std", _S)

        df_enriched = (df_enriched
                       .withColumn("cbu_source", F.lit(cbu_label))
                       .withColumn("source_system", F.lit("NIELSEN"))
                       .withColumn("std_created_at", F.current_timestamp()))
        _fact_frames_std.append(df_enriched)
    except Exception as e:
        warn(True, f"Cannot read AGG_DATA for {cbu_label} ({tbl}): {e}", _S)

if _fact_frames_std:
    df_nielsen_facts_std = _reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True),
                                    _fact_frames_std)
    n_facts_total = df_nielsen_facts_std.count()
    log("INFO", f"nielsen_facts_std: {n_facts_total:,} total rows across {len(_fact_frames_std)} CBUs", _S)
    save_df(df_nielsen_facts_std, "nielsen_facts_std.csv", _S)
else:
    blocker(True, "No Nielsen fact tables could be read — nielsen_facts_std not produced", _S)

flush_log("phase3_standardization_audit_log.txt")
log("INFO", "NIELSEN standardization complete.", _S)

# COMMAND ----------


