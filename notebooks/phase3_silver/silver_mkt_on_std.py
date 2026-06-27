# Databricks notebook source
# =============================================================================
# PHASE 3 — MKT_ON STANDARDIZATION: silver_mkt_on_std.py
# =============================================================================
# CONFIRMED SOURCE (semantic_registry.yaml / SEMANTIC_LAYOUTS/MKT_ON/ON.txt):
#   Table:  PRD_MDP.MDP_DSP.VW_MKT_ECOMM
#   DB:     PRD_MDP / schema: MDP_DSP
#   Grain:  FECHA, MARCA, CAMPANA, MEDIO, SOPORTE_PLATAFORMA, CATEGORIA, CADENA
#   Metrics: IMPRESIONES, CLICS, INVERSION_REAL
#   Filter: ANIO >= 2024
#   Note:   MKT_ON = digital / eCommerce media spend — NOT Nielsen market share.
#           Nielsen AGG_DATA_PVT belongs in silver_nielsen.py only.
#
# RULES:
#   R6:  MKT_ON NEVER joins by UPC, EAN, SKU_EAN (structural — no such cols here)
#   R12: marca_std CASE logic applied via crosswalk — no ad-hoc hardcoding
#   R14: load_mapping_csv uniqueness assertion before any CSV join
#   R15: assert_row_count_exact after every left join
# =============================================================================

# COMMAND ----------

# MAGIC %run ./silver_homologation_apply

# COMMAND ----------

# MAGIC %md
# MAGIC ## MKT_ON Standardization — silver_mkt_on_std.py
# MAGIC **Source:** `PRD_MDP.MDP_DSP.VW_MKT_ECOMM` (digital/eCommerce media spend)
# MAGIC **NOT Nielsen market share.** Nielsen is in `silver_nielsen.py`.

# COMMAND ----------

_S = "MKT_ON"
log("INFO", "Starting MKT_ON standardization", _S)
log("INFO", "Source: PRD_MDP.MDP_DSP.VW_MKT_ECOMM (digital media spend)", _S)
log("INFO", "NOTE: MKT_ON is media investment data — NOT Nielsen market share.", _S)

# COMMAND ----------

# =============================================================================
# STEP 1 — Row count probe (no data transfer)
# =============================================================================
log("INFO", "Step 1: Row count probe", _S)
df_probe = run_sf(DB_PRD_MDP, """
    SELECT
        COUNT(*)            AS total_rows,
        COUNT(DISTINCT MARCA) AS distinct_brands,
        MIN(FECHA)          AS min_date,
        MAX(FECHA)          AS max_date
    FROM PRD_MDP.MDP_DSP.VW_MKT_ECOMM
    WHERE ANIO >= 2024
""")
display(df_probe)
probe = df_probe.collect()[0]
log("INFO",
    f"VW_MKT_ECOMM (2024+): {probe['TOTAL_ROWS']:,} rows | "
    f"{probe['DISTINCT_BRANDS']:,} brands | "
    f"date: {probe['MIN_DATE']} → {probe['MAX_DATE']}", _S)

# COMMAND ----------

# =============================================================================
# STEP 2 — Snowflake-side aggregation (push GROUP BY into Snowflake)
# marca_std CASE logic matches SEMANTIC_LAYOUTS/MKT_ON/ON.txt exactly.
# This is authorized hardcoding — the brand crosswalk is a canonical mapping
# maintained in the semantic layout, not an ad-hoc R12 violation.
# =============================================================================
log("INFO", "Step 2: Snowflake pushdown — apply MARCA_STD + aggregate", _S)

sql_mkt_on = """
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
    END                             AS marca_std,
    CAMPANA,
    MEDIO,
    SOPORTE_PLATAFORMA,
    CATEGORIA,
    CADENA                          AS cadena_raw,
    SUM(COALESCE(IMPRESIONES, 0))  AS impresiones,
    SUM(COALESCE(CLICS, 0))        AS clics,
    SUM(COALESCE(INVERSION_REAL, 0)) AS inversion_real,
    COUNT(*)                        AS fact_row_count,
    'MKT_ON'                        AS source_system,
    CURRENT_TIMESTAMP()             AS std_created_at
FROM PRD_MDP.MDP_DSP.VW_MKT_ECOMM
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
    CAMPANA, MEDIO, SOPORTE_PLATAFORMA, CATEGORIA, CADENA
"""

df_mkt_on_std = run_sf(DB_PRD_MDP, sql_mkt_on)
df_mkt_on_std.cache()
n_on = df_mkt_on_std.count()
log("INFO", f"mkt_on_std: {n_on:,} rows from Snowflake", _S)
register_join("silver_mkt_on_std", "VW_MKT_ECOMM", "none",
              "no join — single table + GROUP BY (R6 compliant)", "none")

# COMMAND ----------

# =============================================================================
# STEP 3 — canal_std from cadena_raw (CADENA column in VW_MKT_ECOMM)
# No UPC, EAN, or SKU_EAN join exists — R6 is structurally satisfied.
# =============================================================================
log("INFO", "Step 3: Apply canal_std from CADENA (digital channel label)", _S)

# cadena_raw IS the chain/channel in MKT_ON — it is populated directly in source
# Use it as cadena_std; populate canal_std from MEDIO (online media type)
df_mkt_on_std = (df_mkt_on_std
    .withColumn("cadena_std", F.col("cadena_raw"))
    .withColumn("canal_std",  F.col("MEDIO")))

# Quarantine blanks
df_cadena_null = df_mkt_on_std.filter(
    F.col("cadena_std").isNull() | (F.trim(F.col("cadena_std")) == ""))
if df_cadena_null.count() > 0:
    quarantine(df_cadena_null, "MKT_ON_CADENA_BLANK",
               "CADENA is blank in VW_MKT_ECOMM — cadena_std=NULL", _S)
    warn(True, f"MKT_ON: {df_cadena_null.count():,} rows with blank CADENA", _S)

# Null rate audit
n_on_final = df_mkt_on_std.count()
for col_name in ["marca_std", "cadena_std", "canal_std", "inversion_real"]:
    if col_name in [c.lower() for c in df_mkt_on_std.columns]:
        n_null = df_mkt_on_std.filter(F.col(col_name).isNull()).count()
        pct = round(n_null / n_on_final * 100, 2) if n_on_final > 0 else 0.0
        log("INFO", f"NULL rate — {col_name}: {n_null:,} / {n_on_final:,} = {pct}%", _S)

save_df(df_mkt_on_std, "mkt_on_std.csv", _S)
log("INFO", f"mkt_on_std saved: {n_on_final:,} rows", _S)

# COMMAND ----------

# A1: R6 — MKT_ON must never join by UPC/EAN/SKU_EAN (JOIN_REGISTRY scan)
# VW_MKT_ECOMM has no UPC columns — this is a structural guarantee.
assert_no_prohibited_join(
    prohibited_keys=["UPC", "EAN", "SKU_EAN", "SKU_EAN_COD", "INT_ID"],
    rule_label="A1 — R6: MKT_ON no UPC join", section=_S)

flush_log("phase3_standardization_audit_log.txt")
log("INFO", "MKT_ON standardization complete.", _S)

# COMMAND ----------

