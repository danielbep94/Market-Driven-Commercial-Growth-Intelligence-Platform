# Databricks notebook source
# =============================================================================
# PHASE 3 — MKT_OFF STANDARDIZATION: silver_mkt_off_std.py
# =============================================================================
# CONFIRMED SOURCE (semantic_registry.yaml / SEMANTIC_LAYOUTS/MKT_OFF/OFF.txt):
#   Table:  PRD_MDP.MDP_STG.FACT_MEDIA_OFF
#   DB:     PRD_MDP / schema: MDP_STG
#   Grain:  FECHA, MARCA, CAMPANA, MEDIO, SOPORTE_PLATAFORMA, CATEGORIA, CLASE
#   Metrics: INVERSION_REAL, IMPACTOS_HT
#   Filter: ANIO >= 2024
#   Note:   MKT_OFF = traditional/offline media spend — TV, radio, print, OOH.
#           There is NO CADENA column in FACT_MEDIA_OFF — only CLASE.
#           cadena_std = NULL is structurally correct (R9).
#
# RULES:
#   R7:  MKT_OFF NEVER joins by UPC (structurally true — no UPC in this table)
#   R8:  MKT_OFF NEVER joins by CADENA (structurally true — CADENA not in FACT_MEDIA_OFF)
#   R9:  cadena_std = NULL — structural (no CADENA column exists in source)
#   R12: marca_std via canonical CASE (same crosswalk as MKT_ON)
#   A4:  Assert cadena_std is NULL for every row
# =============================================================================

# COMMAND ----------

# MAGIC %run ./silver_homologation_apply

# COMMAND ----------

# MAGIC %md
# MAGIC ## MKT_OFF Standardization — silver_mkt_off_std.py
# MAGIC **Source:** `PRD_MDP.MDP_STG.FACT_MEDIA_OFF` (offline/traditional media spend)
# MAGIC **`cadena_std` = NULL** — FACT_MEDIA_OFF has no CADENA column (R9 structural contract).

# COMMAND ----------

_S = "MKT_OFF"
log("INFO", "Starting MKT_OFF standardization", _S)
log("INFO", "Source: PRD_MDP.MDP_STG.FACT_MEDIA_OFF (offline media spend)", _S)
log("INFO", "cadena_std = NULL: FACT_MEDIA_OFF has no CADENA column (R9 — structural)", _S)

# COMMAND ----------

# =============================================================================
# STEP 1 — Row count probe
# =============================================================================
log("INFO", "Step 1: Row count probe", _S)
df_probe = run_sf(DB_PRD_MDP, """
    SELECT
        COUNT(*)              AS total_rows,
        COUNT(DISTINCT MARCA) AS distinct_brands,
        MIN(FECHA)            AS min_date,
        MAX(FECHA)            AS max_date
    FROM PRD_MDP.MDP_STG.FACT_MEDIA_OFF
    WHERE ANIO >= 2024
""")
display(df_probe)
probe = df_probe.collect()[0]
log("INFO",
    f"FACT_MEDIA_OFF (2024+): {probe['TOTAL_ROWS']:,} rows | "
    f"{probe['DISTINCT_BRANDS']:,} brands | "
    f"date: {probe['MIN_DATE']} → {probe['MAX_DATE']}", _S)

# COMMAND ----------

# =============================================================================
# STEP 2 — Snowflake-side aggregation with marca_std
# cadena_std = NULL is architectural (R9) — FACT_MEDIA_OFF has CLASE, not CADENA.
# =============================================================================
log("INFO", "Step 2: Snowflake pushdown — MARCA_STD + aggregate", _S)

sql_mkt_off = """
SELECT
    FECHA,
    MARCA,
    CASE
        WHEN TRIM(UPPER(MARCA)) IN ('ACTIVIA') THEN 'ACTIVIA'
        WHEN TRIM(UPPER(MARCA)) IN ('AGUAS FRESCAS','BFT AGUAS FRESCAS','BONAFONT_AGUASFRESCAS','BONAFONT AGUA FRESCAS') THEN 'AGUAS FRESCAS'
        WHEN TRIM(UPPER(MARCA)) IN ('BADOIT') THEN 'BADOIT'
        WHEN TRIM(UPPER(MARCA)) IN ('BENEGASTRO','BENEG') THEN 'BENEGASTRO'
        WHEN TRIM(UPPER(MARCA)) IN ('BONAFONT','BONAFONT_NATURAL','AZUL BONAFONT') THEN 'BONAFONT'
        WHEN TRIM(UPPER(MARCA)) IN ('BONAFONT JUGO') THEN 'BONAFONT JUGO'
        WHEN TRIM(UPPER(MARCA)) IN ('BONAFONT KIDS','BONAFONT_KIDS','BONAFONT KIDS NATURAL') THEN 'BONAFONT KIDS'
        WHEN TRIM(UPPER(MARCA)) IN ('BONAFONT MINERAL','MINERALIZADA') THEN 'BONAFONT MINERAL'
        WHEN TRIM(UPPER(MARCA)) IN ('BONAFONT SER','SER PURA') THEN 'BONAFONT SER'
        WHEN TRIM(UPPER(MARCA)) IN ('TE BONAFONT','TÉ BONAFONT','BONAFONT TE','BONAFONT_TE','BONAFONT TÉ') THEN 'BONAFONT TE'
        WHEN TRIM(UPPER(MARCA)) IN ('DANETTE') THEN 'DANETTE'
        WHEN TRIM(UPPER(MARCA)) IN ('DANISSIMO') THEN 'DANISSIMO'
        WHEN TRIM(UPPER(MARCA)) IN ('DANMIX','DAN MIX','DANMMIX') THEN 'DANMIX'
        WHEN TRIM(UPPER(MARCA)) IN ('DANONE','DANONE YOGHURT','DANONE CREME','DANONE FREE','DANONE GRIEGO','DANONE FS','DAIRY') THEN 'DANONE'
        WHEN TRIM(UPPER(MARCA)) IN ('DANONINO','DANONINOLIQUIDO') THEN 'DANONINO'
        WHEN TRIM(UPPER(MARCA)) IN ('DANUP','DAN UP','DAN''UP') THEN 'DANUP'
        WHEN TRIM(UPPER(MARCA)) IN ('DANY','DANY DANETTE') THEN 'DANY'
        WHEN TRIM(UPPER(MARCA)) IN ('DELIGHT') THEN 'DELIGHT'
        WHEN TRIM(UPPER(MARCA)) IN ('EVIAN') THEN 'EVIAN'
        WHEN TRIM(UPPER(MARCA)) IN ('HERSHEYS','HERSHEY''S','DANONE HERSHEYS') THEN 'HERSHEYS'
        WHEN TRIM(UPPER(MARCA)) IN ('INFINIT WATER','INFINIT','INFINITY','WATER INFINIT') THEN 'INFINIT'
        WHEN TRIM(UPPER(MARCA)) IN ('JUIZZY') THEN 'JUIZZY'
        WHEN TRIM(UPPER(MARCA)) IN ('LEVITE','BONAFONT_LEVITE','BONAFONT LEVITE') THEN 'LEVITE'
        WHEN TRIM(UPPER(MARCA)) IN ('LICUAMIX') THEN 'LICUAMIX'
        WHEN TRIM(UPPER(MARCA)) IN ('OCEAN SPRAY','OCEAN') THEN 'OCEAN SPRAY'
        WHEN TRIM(UPPER(MARCA)) IN ('OIKOS','OIKOS UHT') THEN 'OIKOS'
        WHEN TRIM(UPPER(MARCA)) IN ('PUREZA AGA','AGA','AGA 20 LTS','AQUAPURA','AGUA NATURAL') THEN 'PUREZA AGA'
        WHEN TRIM(UPPER(MARCA)) IN ('SILK','SILK ORIG 946ML','SILKCHOCO190ML') THEN 'SILK'
        WHEN TRIM(UPPER(MARCA)) IN ('SO DELICIOUS') THEN 'SO DELICIOUS'
        WHEN TRIM(UPPER(MARCA)) IN ('STOK COLD BREW','STOK') THEN 'STOK'
        WHEN TRIM(UPPER(MARCA)) IN ('VITALINEA') THEN 'VITALINEA'
        WHEN TRIM(UPPER(MARCA)) IN ('YOPRO','YO PRO') THEN 'YOPRO'
        WHEN TRIM(UPPER(MARCA)) IN ('COCA COLA','COCA-COLA','COCACOLA','THE COCA COLA EXPORT','COCA COLA FEMSA') THEN 'COCA COLA'
        WHEN TRIM(UPPER(MARCA)) IN ('PEPSI','PEPSI COLA MEXICANA','PEPSICOLA MEXICANA') THEN 'PEPSI'
        WHEN TRIM(UPPER(MARCA)) IN ('LALA','GPO INDUSTRIAL LALA') THEN 'LALA'
        WHEN TRIM(UPPER(MARCA)) IN ('ALPURA') THEN 'ALPURA'
        WHEN TRIM(UPPER(MARCA)) IN ('YOPLAIT') THEN 'YOPLAIT'
        WHEN TRIM(UPPER(MARCA)) IN ('JUMEX','JUGOS DEL VALLE') THEN 'JUMEX'
        WHEN TRIM(UPPER(MARCA)) IN ('SANTA CLARA','SANTA_CLARA') THEN 'SANTA CLARA'
        WHEN TRIM(UPPER(MARCA)) IN ('CIEL','CIEL_EXPRIM','CIEL_MINERAL') THEN 'CIEL'
        WHEN TRIM(UPPER(MARCA)) IN ('PENAFIEL','PEÑAFIEL') THEN 'PENAFIEL'
        WHEN TRIM(UPPER(MARCA)) IN ('SEVEN UP','SEVENUP','SEVEN UP CHI') THEN 'SEVEN UP'
        WHEN TRIM(UPPER(MARCA)) IN ('0','MULTI','DAIRY') THEN '_UNKNOWN'
        WHEN TRIM(UPPER(MARCA)) IN ('MULTIBRAND','MULTIBRAND DAIRY','MULTIBRAND DANONE','MULTIMARCA','INSTITUTO DANONE','DNP','DANONE FS') THEN '_MULTIBRAND'
        ELSE TRIM(UPPER(MARCA))
    END                               AS marca_std,
    CAMPANA,
    MEDIO,
    SOPORTE_PLATAFORMA,
    CATEGORIA,
    CLASE,
    NULL::VARCHAR                     AS cadena_std,  -- R9: structural NULL, FACT_MEDIA_OFF has no CADENA
    SUM(COALESCE(INVERSION_REAL, 0)) AS inversion_real,
    SUM(COALESCE(IMPACTOS_HT, 0))   AS impactos_ht,
    COUNT(*)                          AS fact_row_count,
    'MKT_OFF'                         AS source_system,
    CURRENT_TIMESTAMP()               AS std_created_at
FROM PRD_MDP.MDP_STG.FACT_MEDIA_OFF
WHERE ANIO >= 2024
GROUP BY
    FECHA, MARCA,
    CASE
        WHEN TRIM(UPPER(MARCA)) IN ('ACTIVIA') THEN 'ACTIVIA'
        WHEN TRIM(UPPER(MARCA)) IN ('AGUAS FRESCAS','BFT AGUAS FRESCAS','BONAFONT_AGUASFRESCAS','BONAFONT AGUA FRESCAS') THEN 'AGUAS FRESCAS'
        WHEN TRIM(UPPER(MARCA)) IN ('BADOIT') THEN 'BADOIT'
        WHEN TRIM(UPPER(MARCA)) IN ('BENEGASTRO','BENEG') THEN 'BENEGASTRO'
        WHEN TRIM(UPPER(MARCA)) IN ('BONAFONT','BONAFONT_NATURAL','AZUL BONAFONT') THEN 'BONAFONT'
        WHEN TRIM(UPPER(MARCA)) IN ('BONAFONT JUGO') THEN 'BONAFONT JUGO'
        WHEN TRIM(UPPER(MARCA)) IN ('BONAFONT KIDS','BONAFONT_KIDS','BONAFONT KIDS NATURAL') THEN 'BONAFONT KIDS'
        WHEN TRIM(UPPER(MARCA)) IN ('BONAFONT MINERAL','MINERALIZADA') THEN 'BONAFONT MINERAL'
        WHEN TRIM(UPPER(MARCA)) IN ('BONAFONT SER','SER PURA') THEN 'BONAFONT SER'
        WHEN TRIM(UPPER(MARCA)) IN ('TE BONAFONT','TÉ BONAFONT','BONAFONT TE','BONAFONT_TE','BONAFONT TÉ') THEN 'BONAFONT TE'
        WHEN TRIM(UPPER(MARCA)) IN ('DANETTE') THEN 'DANETTE'
        WHEN TRIM(UPPER(MARCA)) IN ('DANISSIMO') THEN 'DANISSIMO'
        WHEN TRIM(UPPER(MARCA)) IN ('DANMIX','DAN MIX','DANMMIX') THEN 'DANMIX'
        WHEN TRIM(UPPER(MARCA)) IN ('DANONE','DANONE YOGHURT','DANONE CREME','DANONE FREE','DANONE GRIEGO','DANONE FS','DAIRY') THEN 'DANONE'
        WHEN TRIM(UPPER(MARCA)) IN ('DANONINO','DANONINOLIQUIDO') THEN 'DANONINO'
        WHEN TRIM(UPPER(MARCA)) IN ('DANUP','DAN UP','DAN''UP') THEN 'DANUP'
        WHEN TRIM(UPPER(MARCA)) IN ('DANY','DANY DANETTE') THEN 'DANY'
        WHEN TRIM(UPPER(MARCA)) IN ('DELIGHT') THEN 'DELIGHT'
        WHEN TRIM(UPPER(MARCA)) IN ('EVIAN') THEN 'EVIAN'
        WHEN TRIM(UPPER(MARCA)) IN ('HERSHEYS','HERSHEY''S','DANONE HERSHEYS') THEN 'HERSHEYS'
        WHEN TRIM(UPPER(MARCA)) IN ('INFINIT WATER','INFINIT','INFINITY','WATER INFINIT') THEN 'INFINIT'
        WHEN TRIM(UPPER(MARCA)) IN ('JUIZZY') THEN 'JUIZZY'
        WHEN TRIM(UPPER(MARCA)) IN ('LEVITE','BONAFONT_LEVITE','BONAFONT LEVITE') THEN 'LEVITE'
        WHEN TRIM(UPPER(MARCA)) IN ('LICUAMIX') THEN 'LICUAMIX'
        WHEN TRIM(UPPER(MARCA)) IN ('OCEAN SPRAY','OCEAN') THEN 'OCEAN SPRAY'
        WHEN TRIM(UPPER(MARCA)) IN ('OIKOS','OIKOS UHT') THEN 'OIKOS'
        WHEN TRIM(UPPER(MARCA)) IN ('PUREZA AGA','AGA','AGA 20 LTS','AQUAPURA','AGUA NATURAL') THEN 'PUREZA AGA'
        WHEN TRIM(UPPER(MARCA)) IN ('SILK','SILK ORIG 946ML','SILKCHOCO190ML') THEN 'SILK'
        WHEN TRIM(UPPER(MARCA)) IN ('SO DELICIOUS') THEN 'SO DELICIOUS'
        WHEN TRIM(UPPER(MARCA)) IN ('STOK COLD BREW','STOK') THEN 'STOK'
        WHEN TRIM(UPPER(MARCA)) IN ('VITALINEA') THEN 'VITALINEA'
        WHEN TRIM(UPPER(MARCA)) IN ('YOPRO','YO PRO') THEN 'YOPRO'
        WHEN TRIM(UPPER(MARCA)) IN ('COCA COLA','COCA-COLA','COCACOLA','THE COCA COLA EXPORT','COCA COLA FEMSA') THEN 'COCA COLA'
        WHEN TRIM(UPPER(MARCA)) IN ('PEPSI','PEPSI COLA MEXICANA','PEPSICOLA MEXICANA') THEN 'PEPSI'
        WHEN TRIM(UPPER(MARCA)) IN ('LALA','GPO INDUSTRIAL LALA') THEN 'LALA'
        WHEN TRIM(UPPER(MARCA)) IN ('ALPURA') THEN 'ALPURA'
        WHEN TRIM(UPPER(MARCA)) IN ('YOPLAIT') THEN 'YOPLAIT'
        WHEN TRIM(UPPER(MARCA)) IN ('JUMEX','JUGOS DEL VALLE') THEN 'JUMEX'
        WHEN TRIM(UPPER(MARCA)) IN ('SANTA CLARA','SANTA_CLARA') THEN 'SANTA CLARA'
        WHEN TRIM(UPPER(MARCA)) IN ('CIEL','CIEL_EXPRIM','CIEL_MINERAL') THEN 'CIEL'
        WHEN TRIM(UPPER(MARCA)) IN ('PENAFIEL','PEÑAFIEL') THEN 'PENAFIEL'
        WHEN TRIM(UPPER(MARCA)) IN ('SEVEN UP','SEVENUP','SEVEN UP CHI') THEN 'SEVEN UP'
        WHEN TRIM(UPPER(MARCA)) IN ('0','MULTI','DAIRY') THEN '_UNKNOWN'
        WHEN TRIM(UPPER(MARCA)) IN ('MULTIBRAND','MULTIBRAND DAIRY','MULTIBRAND DANONE','MULTIMARCA','INSTITUTO DANONE','DNP','DANONE FS') THEN '_MULTIBRAND'
        ELSE TRIM(UPPER(MARCA))
    END,
    CAMPANA, MEDIO, SOPORTE_PLATAFORMA, CATEGORIA, CLASE
"""

df_mkt_off_std = run_sf(DB_PRD_MDP, sql_mkt_off)
df_mkt_off_std.cache()
n_off = df_mkt_off_std.count()
log("INFO", f"mkt_off_std: {n_off:,} rows from Snowflake", _S)
register_join("silver_mkt_off_std", "FACT_MEDIA_OFF", "none",
              "no join — single table + GROUP BY (R7/R8 compliant)", "none")

# Null rate audit
for col_name in ["marca_std", "cadena_std", "inversion_real"]:
    if col_name in [c.lower() for c in df_mkt_off_std.columns]:
        n_null = df_mkt_off_std.filter(F.col(col_name).isNull()).count()
        pct = round(n_null / n_off * 100, 2) if n_off > 0 else 0.0
        log("INFO", f"NULL rate — {col_name}: {n_null:,} / {n_off:,} = {pct}%", _S)

save_df(df_mkt_off_std, "mkt_off_std.csv", _S)
log("INFO", f"mkt_off_std saved: {n_off:,} rows", _S)

# COMMAND ----------

# A4: cadena_std must be NULL for ALL rows (R9 — structural, FACT_MEDIA_OFF has no CADENA)
n_non_null = df_mkt_off_std.filter(F.col("cadena_std").isNotNull()).count()
blocker(n_non_null > 0,
    f"A4 VIOLATION: {n_non_null:,} rows in mkt_off_std have non-NULL cadena_std. "
    "FACT_MEDIA_OFF has no CADENA column — cadena_std must be NULL (R9).", _S)
if n_non_null == 0:
    passed(f"A4: cadena_std is NULL for all {n_off:,} mkt_off_std rows (R9 confirmed)", _S)

# A2: R7 — MKT_OFF no UPC join | A3: R8 — MKT_OFF no CADENA join
_backup = JOIN_REGISTRY.copy()
_off_entries = [e for e in JOIN_REGISTRY if e["notebook"] == "silver_mkt_off_std"]
JOIN_REGISTRY.clear(); JOIN_REGISTRY.extend(_off_entries)
assert_no_prohibited_join(["UPC","EAN","SKU_EAN","SKU_EAN_COD","INT_ID"],
                          "A2 — R7: MKT_OFF no UPC join", _S)
assert_no_prohibited_join(["CADENA","cadena_std","CUS_CADENA"],
                          "A3 — R8: MKT_OFF no CADENA join", _S)
JOIN_REGISTRY.clear(); JOIN_REGISTRY.extend(_backup)

flush_log("phase3_standardization_audit_log.txt")
log("INFO", "MKT_OFF standardization complete.", _S)

# COMMAND ----------

