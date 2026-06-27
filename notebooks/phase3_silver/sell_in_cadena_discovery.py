# Databricks notebook source
# =============================================================================
# M2 — SELL_IN CADENA DISCOVERY: sell_in_cadena_discovery.py
# =============================================================================
# PURPOSE:
#   Find the best cadena (retail chain) column for SELL_IN from:
#     1. VW_FACT_RNV  — the sell-in fact table
#     2. V_D_CLIENT   — the customer dimension
#     3. VW_D_CUSTOMER_DICTONARY — the customer harmonization bridge
#
# OUTPUT (written to logs/):
#   m2_v_d_client_columns.csv        — all V_D_CLIENT columns + cardinality
#   m2_v_d_client_samples.csv        — top 20 values for each candidate column
#   m2_fact_rnv_customer_columns.csv — customer-related columns in fact table
#   m2_customer_dict_columns.csv     — VW_D_CUSTOMER_DICTONARY cardinality
#   m2_cadena_recommendation.txt     — ranked recommendations
#
# DESIGN:
#   - ALL queries are read-only (SELECT only)
#   - Large tables sampled via LIMIT or GROUP BY COUNT — never full scan
#   - Candidate columns identified by cardinality:
#       < 15 distinct values  → likely CANAL (grand channel)
#       15 – 200             → likely CADENA (retail chain) ← TARGET
#       > 200                → likely customer ID or description (too granular)
# =============================================================================

# COMMAND ----------

# MAGIC %run ./silver_homologation_apply

# COMMAND ----------

# MAGIC %md
# MAGIC ## M2 — SELL_IN Cadena Discovery
# MAGIC **Goal:** Find which column(s) in `V_D_CLIENT` / `VW_FACT_RNV` represent
# MAGIC the retail chain (cadena) — NOT just the grand channel (canal).
# MAGIC
# MAGIC All queries are **READ-ONLY**. No Snowflake tables are modified.

# COMMAND ----------

_S = "M2_CADENA_DISCOVERY"
log("INFO", "Starting M2 SELL_IN cadena discovery", _S)
log("INFO", "READ-ONLY: no Snowflake tables will be modified", _S)

CARDINALITY_LOW  = 15   # <= 15 → likely CANAL (grand channel)
CARDINALITY_HIGH = 200  # >= 200 → too granular (customer ID or name)

# COMMAND ----------

# =============================================================================
# SECTION 1 — V_D_CLIENT full schema + cardinality for every TEXT column
# =============================================================================
log("INFO", "Section 1: V_D_CLIENT — all columns and cardinality", _S)

# Step 1A: Get all columns from INFORMATION_SCHEMA
df_schema = run_sf(DB_PRD_MEX, """
    SELECT
        COLUMN_NAME,
        DATA_TYPE,
        CHARACTER_MAXIMUM_LENGTH,
        IS_NULLABLE,
        ORDINAL_POSITION
    FROM PRD_MEX.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'MEX_DSP_OTC'
      AND TABLE_NAME   = 'V_D_CLIENT'
    ORDER BY ORDINAL_POSITION
""")
df_schema.cache()
n_cols = df_schema.count()
log("INFO", f"V_D_CLIENT has {n_cols} columns", _S)
display(df_schema)
save_df(df_schema, "m2_v_d_client_schema.csv", _S)

# Step 1B: Cardinality of every TEXT/VARCHAR column
# We do this in a single SQL to avoid N round-trips
text_cols = [
    r["COLUMN_NAME"] for r in df_schema.collect()
    if r["DATA_TYPE"] in ("TEXT", "VARCHAR", "STRING", "CHAR")
]
log("INFO", f"V_D_CLIENT TEXT columns to cardinalize: {len(text_cols)}", _S)

cardinality_rows = []
for col in text_cols:
    try:
        df_card = run_sf(DB_PRD_MEX, f"""
            SELECT
                '{col}'                                   AS column_name,
                COUNT(DISTINCT TO_VARCHAR({col}))         AS distinct_values,
                COUNT_IF({col} IS NULL)                   AS null_count,
                COUNT(*)                                  AS total_count
            FROM PRD_MEX.MEX_DSP_OTC.V_D_CLIENT
        """)
        row = df_card.collect()[0]
        n_dist = row["DISTINCT_VALUES"]
        cardinality_rows.append({
            "column_name":    col,
            "distinct_values": n_dist,
            "null_count":     row["NULL_COUNT"],
            "total_count":    row["TOTAL_COUNT"],
            "cardinality_class": (
                "CANAL_CANDIDATE"   if n_dist <= CARDINALITY_LOW else
                "CADENA_CANDIDATE"  if n_dist <= CARDINALITY_HIGH else
                "TOO_GRANULAR"
            )
        })
        log("INFO", f"  {col}: {n_dist} distinct → "
            f"{'⭐ CADENA_CANDIDATE' if CARDINALITY_LOW < n_dist <= CARDINALITY_HIGH else 'skip'}",
            _S)
    except Exception as e:
        log("INFO", f"  {col}: could not cardinalize — {e}", _S)

df_cardinality = spark.createDataFrame(cardinality_rows)
display(df_cardinality.orderBy("distinct_values"))
save_df(df_cardinality, "m2_v_d_client_columns.csv", _S)

# COMMAND ----------

# =============================================================================
# SECTION 2 — Sample top-20 values for every CADENA_CANDIDATE column
# =============================================================================
log("INFO", "Section 2: Sample top-20 values for CADENA_CANDIDATE columns", _S)

cadena_candidates = [
    r["column_name"] for r in df_cardinality.collect()
    if r["cardinality_class"] == "CADENA_CANDIDATE"
]
log("INFO", f"Cadena candidates: {cadena_candidates}", _S)

sample_rows = []
for col in cadena_candidates:
    try:
        df_samp = run_sf(DB_PRD_MEX, f"""
            SELECT
                '{col}'                   AS column_name,
                TO_VARCHAR({col})         AS value,
                COUNT(*)                  AS row_count
            FROM PRD_MEX.MEX_DSP_OTC.V_D_CLIENT
            GROUP BY TO_VARCHAR({col})
            ORDER BY row_count DESC
            LIMIT 20
        """)
        rows = df_samp.collect()
        for r in rows:
            sample_rows.append({
                "column_name": col,
                "value":       r["VALUE"],
                "row_count":   r["ROW_COUNT"]
            })
    except Exception as e:
        log("INFO", f"  Sample failed for {col}: {e}", _S)

if sample_rows:
    df_samples = spark.createDataFrame(sample_rows)
    display(df_samples.orderBy("column_name", "row_count"))
    save_df(df_samples, "m2_v_d_client_samples.csv", _S)
    log("INFO", f"Saved {len(sample_rows)} sample rows for {len(cadena_candidates)} candidates", _S)
else:
    log("INFO", "No CADENA_CANDIDATE columns found in V_D_CLIENT", _S)

# COMMAND ----------

# =============================================================================
# SECTION 3 — VW_D_CUSTOMER_DICTONARY cardinality
# (This is the harmonization bridge — may contain chain-level fields)
# =============================================================================
log("INFO", "Section 3: VW_D_CUSTOMER_DICTONARY column cardinality", _S)

df_dict_schema = run_sf(DB_PRD_MEX, """
    SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION
    FROM PRD_MEX.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'MEX_DSP_OTC'
      AND TABLE_NAME   = 'VW_D_CUSTOMER_DICTONARY'
    ORDER BY ORDINAL_POSITION
""")
dict_cols = [
    r["COLUMN_NAME"] for r in df_dict_schema.collect()
    if r["DATA_TYPE"] in ("TEXT", "VARCHAR", "STRING", "CHAR")
]
log("INFO", f"VW_D_CUSTOMER_DICTONARY TEXT columns: {dict_cols}", _S)

dict_rows = []
for col in dict_cols:
    try:
        df_dc = run_sf(DB_PRD_MEX, f"""
            SELECT '{col}' AS column_name,
                   COUNT(DISTINCT TO_VARCHAR({col})) AS distinct_values,
                   COUNT(*) AS total_count
            FROM PRD_MEX.MEX_DSP_OTC.VW_D_CUSTOMER_DICTONARY
        """)
        row = df_dc.collect()[0]
        n_dist = row["DISTINCT_VALUES"]
        dict_rows.append({
            "column_name":     col,
            "distinct_values": n_dist,
            "total_count":     row["TOTAL_COUNT"],
            "cardinality_class": (
                "CANAL_CANDIDATE"  if n_dist <= CARDINALITY_LOW else
                "CADENA_CANDIDATE" if n_dist <= CARDINALITY_HIGH else
                "TOO_GRANULAR"
            )
        })
        if CARDINALITY_LOW < n_dist <= CARDINALITY_HIGH:
            log("INFO", f"  ⭐ {col}: {n_dist} distinct → CADENA_CANDIDATE", _S)
    except Exception as e:
        log("INFO", f"  {col}: error — {e}", _S)

if dict_rows:
    df_dict_card = spark.createDataFrame(dict_rows)
    display(df_dict_card.orderBy("distinct_values"))
    save_df(df_dict_card, "m2_customer_dict_columns.csv", _S)

# COMMAND ----------

# =============================================================================
# SECTION 4 — VW_FACT_RNV customer-linkage columns
# Confirm which column in fact links to V_D_CLIENT
# =============================================================================
log("INFO", "Section 4: VW_FACT_RNV — customer key columns and cardinality", _S)

# Confirmed customer-related columns from column_types_snapshot.yaml:
# SHP_CUS_IDT (TEXT), PAY_CUS_IDT (NUMBER), DIS_CHL_COD (TEXT),
# SAL_ORG_COD (TEXT), CUS_SAL_RGN_COD, CUS_SAL_GRP_COD, etc.
fact_customer_cols = [
    "SHP_CUS_IDT", "PAY_CUS_IDT", "DIS_CHL_COD",
    "SAL_ORG_COD", "CUS_SAL_RGN_COD", "CUS_1ST_IND_COD",
    "CUS_IND_KEY_COD", "CUS_SAL_GRP_COD", "CUS_SAL_OFI_COD",
    "CUS_ABC_CLS_COD", "CUS_PRC_LIS_TYP_COD"
]

fact_rows = []
for col in fact_customer_cols:
    try:
        df_fc = run_sf(DB_PRD_MEX, f"""
            SELECT '{col}' AS column_name,
                   COUNT(DISTINCT TO_VARCHAR({col})) AS distinct_values,
                   COUNT(*) AS total_count
            FROM PRD_MEX.MEX_DSP_OTC.VW_FACT_RNV
        """)
        row = df_fc.collect()[0]
        n_dist = row["DISTINCT_VALUES"]
        fact_rows.append({
            "column_name":     col,
            "distinct_values": n_dist,
            "total_count":     row["TOTAL_COUNT"],
            "cardinality_class": (
                "CANAL_CANDIDATE"  if n_dist <= CARDINALITY_LOW else
                "CADENA_CANDIDATE" if n_dist <= CARDINALITY_HIGH else
                "TOO_GRANULAR"
            ),
            "note": "customer key in fact → joins to V_D_CLIENT"
        })
        log("INFO", f"  {col}: {n_dist} distinct", _S)
    except Exception as e:
        log("INFO", f"  {col}: error — {e}", _S)

if fact_rows:
    df_fact_card = spark.createDataFrame(fact_rows)
    display(df_fact_card.orderBy("distinct_values"))
    save_df(df_fact_card, "m2_fact_rnv_customer_columns.csv", _S)

# COMMAND ----------

# =============================================================================
# SECTION 5 — Join probe: SHP_CUS_IDT → V_D_CLIENT to verify link + find cadena
# =============================================================================
log("INFO", "Section 5: Join probe — fact.SHP_CUS_IDT → V_D_CLIENT", _S)

# Build candidate cadena columns to show from V_D_CLIENT
# We'll SELECT all TEXT columns with cardinality between 15-200
probe_cols_str = ", ".join([
    f"c.{col} AS cli_{col.lower()}" for col in cadena_candidates[:10]
]) if cadena_candidates else "NULL AS cli_no_candidates"

df_join_probe = run_sf(DB_PRD_MEX, f"""
    SELECT
        f.SHP_CUS_IDT,
        c.CUS_IDT,
        c.CUS_GRN_CHL_DSC,
        c.CUS_NAM_DSC,
        {probe_cols_str},
        COUNT(*) AS fact_rows
    FROM PRD_MEX.MEX_DSP_OTC.VW_FACT_RNV f
    LEFT JOIN PRD_MEX.MEX_DSP_OTC.V_D_CLIENT c
        ON TO_VARCHAR(f.SHP_CUS_IDT) = TO_VARCHAR(c.CUS_IDT)
    WHERE f.BIL_DAT >= 20250101
    GROUP BY
        f.SHP_CUS_IDT,
        c.CUS_IDT,
        c.CUS_GRN_CHL_DSC,
        c.CUS_NAM_DSC
        {', ' + ', '.join(['c.' + col for col in cadena_candidates[:10]]) if cadena_candidates else ''}
    ORDER BY fact_rows DESC
    LIMIT 200
""")
display(df_join_probe)
save_df(df_join_probe, "m2_join_probe_shpcus_to_client.csv", _S)
n_probe = df_join_probe.count()
n_matched = df_join_probe.filter(F.col("CUS_IDT").isNotNull()).count()
match_pct = round(n_matched / n_probe * 100, 1) if n_probe > 0 else 0
log("INFO", f"Join probe: {n_matched}/{n_probe} ({match_pct}%) SHP_CUS_IDT → V_D_CLIENT matched", _S)

# COMMAND ----------

# DBTITLE 1,Cell 10
# =============================================================================
# SECTION 6 — Written recommendation
# =============================================================================
log("INFO", "Section 6: Writing cadena recommendation", _S)

all_candidates = [r for r in (dict_rows or []) + (
    [r for r in (cardinality_rows if cardinality_rows else [])
     if r["cardinality_class"] == "CADENA_CANDIDATE"]
) ]

rec_lines = [
    "=" * 70,
    "M2 SELL_IN CADENA DISCOVERY — RECOMMENDATION REPORT",
    f"Generated: {datetime.datetime.now()}",
    "=" * 70,
    "",
    f"Cardinality thresholds: CANAL ≤{CARDINALITY_LOW} | CADENA {CARDINALITY_LOW+1}–{CARDINALITY_HIGH} | TOO_GRANULAR >{CARDINALITY_HIGH}",
    "",
    "── V_D_CLIENT CADENA_CANDIDATE columns ─────────────────────────────",
]
v_d_cands = [r for r in (cardinality_rows if cardinality_rows else [])
             if r["cardinality_class"] == "CADENA_CANDIDATE"]
if v_d_cands:
    for r in sorted(v_d_cands, key=lambda x: x["distinct_values"]):
        rec_lines.append(
            f"  {r['column_name']:40s} {int(r['distinct_values']):4d} distinct → CADENA_CANDIDATE"
        )
else:
    rec_lines.append("  ⚠️  No columns in V_D_CLIENT fall in the 15–200 cardinality range.")
    rec_lines.append("  → CUS_GRN_CHL_DSC (≤10 values) = CANAL (grand channel) — not cadena level.")
    rec_lines.append("  → cadena_std = NULL for SELL_IN is architecturally correct (same as MKT_OFF).")

rec_lines += [
    "",
    "── VW_D_CUSTOMER_DICTONARY CADENA_CANDIDATE columns ────────────────",
]
dict_cands = [r for r in (dict_rows if dict_rows else [])
              if r["cardinality_class"] == "CADENA_CANDIDATE"]
if dict_cands:
    for r in sorted(dict_cands, key=lambda x: x["distinct_values"]):
        rec_lines.append(
            f"  {r['column_name']:40s} {int(r['distinct_values']):4d} distinct → CADENA_CANDIDATE"
        )
else:
    rec_lines.append("  No cadena-level columns found in VW_D_CUSTOMER_DICTONARY either.")

rec_lines += [
    "",
    "── SHP_CUS_IDT → V_D_CLIENT join match rate ────────────────────────",
    f"  {n_matched}/{n_probe} sampled rows matched ({match_pct}%)",
    "",
    "── FINAL RECOMMENDATION ────────────────────────────────────────────",
]
if v_d_cands or dict_cands:
    best = (v_d_cands + dict_cands)[0]
    rec_lines.append(
        f"  USE: {best['column_name']} ({best['distinct_values']} distinct values)"
    )
    rec_lines.append(
        "  Wire this column as cadena_raw in silver_sell_in.py and map to cadena_std."
    )
else:
    rec_lines.append(
        "  CONCLUSION: No cadena-level column found in V_D_CLIENT or VW_D_CUSTOMER_DICTONARY."
    )
    rec_lines.append(
        "  cadena_std = NULL for SELL_IN is architecturally correct."
    )
    rec_lines.append(
        "  This matches the MKT_OFF pattern (R9) — document as structural NULL, not a gap."
    )

rec_text = "\n".join(rec_lines)
print(rec_text)

rec_path = os.path.join(REPO_LOGS_DIR, "m2_cadena_recommendation.txt")
with open(rec_path, "w") as f:
    f.write(rec_text)
log("INFO", f"Recommendation written to {rec_path}", _S)
flush_log("phase3_standardization_audit_log.txt")
log("INFO", "M2 cadena discovery complete.", _S)
