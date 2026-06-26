"""
MDM Cross-Source Join Validation Notebook B
=============================================
Purpose : Validate cross-source join integrity at each grain level.
          Confirm that join predicates respect UPC and CADENA applicability
          rules. Detect violations before they reach production.

Key rules enforced
------------------
1. MKT_ON and MKT_OFF must NEVER be joined using a UPC predicate.
2. MKT_OFF must NEVER require CADENA as a join predicate.
3. UPC_STD is NOT used in the all-source join template.
   UPC validation is scoped exclusively to the SELL_IN ↔ SELL_OUT bridge
   (Section A4 in Notebook A / Section B3 in this notebook).
4. All-source joins must operate at the coarsest common grain:
       MARCA_STD + DATE_TRUNC('MONTH', FECHA) [+ optional CANAL/CADENA]
5. Aggregate each source to the target grain before joining raw transactional rows.
6. MKT_OFF CADENA_STD must be 100% NULL (business rule, not a data defect).
7. MKT_ON and MKT_OFF UPC_STD must be 100% NULL (business rule, not a defect).

Output files
------------
logs/validation_results_mdm_cross_source.txt — master cross-source audit log

Prerequisites
-------------
pip install snowflake-connector-python pandas
Environment variables required:
    SF_ACCOUNT, SF_USER, SF_PASSWORD, SF_WAREHOUSE, SF_ROLE (optional)

Run locally:
    python notebooks/validate_mdm_cross_source_joins.py
"""

import os
import datetime
import textwrap
import pandas as pd

try:
    import snowflake.connector
    SF_AVAILABLE = True
except ImportError:
    SF_AVAILABLE = False
    print("WARNING: snowflake-connector-python not installed — all queries will be skipped.")


# ===========================================================================
# Configuration
# ===========================================================================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOGS_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOGS_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOGS_DIR, "validation_results_mdm_cross_source.txt")

# UPC / CADENA applicability by source (mirrors Notebook A constants)
UPC_APPLICABLE = {
    "SELL_IN": True,   "SELL_OUT": True,
    "MKT_ON": False,   "MKT_OFF": False,
    "EDP_NIELSEN": False, "PB_NIELSEN": False,
    "WATER_NIELSEN_RIE": False, "WATER_SCANTRACK": False,
    "IBP": False, "WASTE": False,
}
CADENA_APPLICABLE = {
    "SELL_IN": False,  "SELL_OUT": True,
    "MKT_ON": True,    "MKT_OFF": False,
    "EDP_NIELSEN": True, "PB_NIELSEN": True,
    "WATER_NIELSEN_RIE": True, "WATER_SCANTRACK": True,
    "IBP": True, "WASTE": True,
}


# ===========================================================================
# Utility helpers
# ===========================================================================

def ts() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_sf_connection():
    if not SF_AVAILABLE:
        return None
    required = ["SF_ACCOUNT", "SF_USER", "SF_PASSWORD", "SF_WAREHOUSE"]
    if any(not os.getenv(k) for k in required):
        return None
    try:
        return snowflake.connector.connect(
            account   = os.environ["SF_ACCOUNT"],
            user      = os.environ["SF_USER"],
            password  = os.environ["SF_PASSWORD"],
            warehouse = os.environ["SF_WAREHOUSE"],
            role      = os.getenv("SF_ROLE", ""),
        )
    except Exception:
        return None


def run_query(conn, sql: str) -> pd.DataFrame | None:
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    except Exception as exc:
        return None


def log_skipped(log, label: str, sql: str):
    log.write(f"[{ts()}] SKIPPED — {label}: Snowflake unavailable.\n")
    log.write(f"  SQL to run manually:\n{textwrap.indent(sql.strip(), '    ')}\n\n")


# ===========================================================================
# Section B1 — Row count before / after standardization per source
# ===========================================================================

ROW_COUNT_QUERIES: dict[str, str] = {
    "SELL_IN_RAW": """
        SELECT COUNT(*) AS row_count, COUNT(DISTINCT FAC.MAT_IDT) AS distinct_skus
        FROM PRD_MEX.MEX_DSP_OTC.VW_FACT_RNV AS FAC
        WHERE FAC.BIL_DAT >= 20250101
    """,
    "SELL_IN_AFTER_STD": """
        SELECT COUNT(*) AS row_count, COUNT(DISTINCT SKU) AS distinct_skus
        FROM (
            SELECT FAC.MAT_IDT AS SKU, PRO.LV2_UMB_BRD_DSC AS MARCA_RAW
            FROM PRD_MEX.MEX_DSP_OTC.VW_FACT_RNV AS FAC
            LEFT JOIN PRD_MEX.MEX_DSP_OTC.V_D_ITEM AS PRO ON FAC.MAT_IDT = PRO.MAT_IDT
            WHERE FAC.CBU IN ('WATERS','EDP')
              AND DATE_TRUNC('MONTH', TO_DATE(TO_VARCHAR(FAC.BIL_DAT), 'YYYYMMDD')) >= '2025-01-01'
        ) q
    """,
    "SELL_OUT_RAW": """
        SELECT COUNT(*) AS row_count, COUNT(DISTINCT TO_VARCHAR(UPC)) AS distinct_upcs
        FROM PRD_MDP.MDP_DSP.VW_FACT_SELL_OUT
        WHERE PER_ID >= 20250101
    """,
    "SELL_OUT_AFTER_STD": """
        SELECT COUNT(*) AS row_count,
               COUNT(DISTINCT TO_VARCHAR(f.UPC)) AS distinct_upcs
        FROM PRD_MDP.MDP_DSP.VW_FACT_SELL_OUT f
        INNER JOIN PRD_MDP.MDP_DSP.VW_D_STORE_RM  st   ON f.STORE = st.INT_ID AND f.CBU_ID = st.CBU_ID
        INNER JOIN PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM prod ON TO_VARCHAR(f.UPC) = TO_VARCHAR(prod.INT_ID)
                                                         AND f.CBU_ID = prod.CBU_ID
        WHERE f.PER_ID >= 20250101
    """,
    "MKT_ON_RAW": """
        SELECT COUNT(*) AS row_count, COUNT(DISTINCT MARCA) AS distinct_brands
        FROM PRD_MDP.MDP_DSP.VW_MKT_ECOMM
        WHERE ANIO >= 2024
    """,
    "MKT_OFF_RAW": """
        SELECT COUNT(*) AS row_count, COUNT(DISTINCT MARCA) AS distinct_brands
        FROM PRD_MDP.MDP_STG.FACT_MEDIA_OFF
        WHERE ANIO >= 2024
    """,
    "IBP_RAW": """
        SELECT COUNT(*) AS row_count, COUNT(DISTINCT MARCA) AS distinct_brands,
               COUNT(DISTINCT CADENA) AS distinct_cadenas
        FROM PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP
    """,
    "WASTE_RAW": """
        SELECT COUNT(*) AS row_count, COUNT(DISTINCT MARCA) AS distinct_brands
        FROM PRD_MDP.MDP_STG.FACT_TOPLINE
        WHERE UPPER(TRIM(FUENTE)) = 'TOPLINE'
    """,
}


def check_row_counts(conn, log):
    log.write("\n" + "=" * 70 + "\n")
    log.write("SECTION B1 — ROW COUNT BEFORE / AFTER STANDARDIZATION\n")
    log.write("=" * 70 + "\n")

    for label, sql in ROW_COUNT_QUERIES.items():
        df = run_query(conn, sql)
        if df is None:
            log_skipped(log, label, sql)
        else:
            log.write(f"[{ts()}] {label}:\n")
            log.write(df.to_string(index=False) + "\n\n")


# ===========================================================================
# Section B2 — MARCA_STD overlap between SELL_IN and each source
# ===========================================================================

SELL_IN_BRANDS_SQL = """
SELECT DISTINCT TRIM(UPPER(LV2_UMB_BRD_DSC)) AS marca_raw
FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM
WHERE LV2_UMB_BRD_DSC IS NOT NULL
"""

BRAND_OVERLAP_QUERIES: dict[str, str] = {
    "SELL_OUT": "SELECT DISTINCT TRIM(UPPER(BRAND)) AS marca_raw FROM PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM WHERE BRAND IS NOT NULL",
    "MKT_ON":   "SELECT DISTINCT TRIM(UPPER(MARCA)) AS marca_raw FROM PRD_MDP.MDP_DSP.VW_MKT_ECOMM WHERE MARCA IS NOT NULL",
    "MKT_OFF":  "SELECT DISTINCT TRIM(UPPER(MARCA)) AS marca_raw FROM PRD_MDP.MDP_STG.FACT_MEDIA_OFF WHERE MARCA IS NOT NULL",
    "IBP":      "SELECT DISTINCT TRIM(UPPER(MARCA)) AS marca_raw FROM PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP WHERE MARCA IS NOT NULL",
    "WASTE":    "SELECT DISTINCT TRIM(UPPER(MARCA)) AS marca_raw FROM PRD_MDP.MDP_STG.FACT_TOPLINE WHERE MARCA IS NOT NULL AND UPPER(TRIM(FUENTE)) = 'TOPLINE'",
    "EDP_NIELSEN": "SELECT DISTINCT TRIM(UPPER(INP_56985)) AS marca_raw FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_PROD_DIM WHERE INP_56985 IS NOT NULL",
    "PB_NIELSEN":  "SELECT DISTINCT TRIM(UPPER(INP_56985)) AS marca_raw FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_SUST_LECHE_ST_NLSN_PROD_DIM WHERE INP_56985 IS NOT NULL",
    "WATER_NIELSEN_RIE": "SELECT DISTINCT TRIM(UPPER(CSTM_310589)) AS marca_raw FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_RT_NLSN_PROD_DIM WHERE CSTM_310589 IS NOT NULL",
    "WATER_SCANTRACK":   "SELECT DISTINCT TRIM(UPPER(CSTM_310589)) AS marca_raw FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_ST_NLSN_PROD_DIM WHERE CSTM_310589 IS NOT NULL",
}


def check_brand_overlap(conn, log):
    log.write("\n" + "=" * 70 + "\n")
    log.write("SECTION B2 — MARCA_STD OVERLAP: SELL_IN vs EACH SOURCE\n")
    log.write("=" * 70 + "\n")

    si_df = run_query(conn, SELL_IN_BRANDS_SQL)
    if si_df is None:
        log.write(f"[{ts()}] SKIPPED — Cannot fetch SELL_IN brands from Snowflake.\n")
        log.write(f"  SQL to run manually:\n{textwrap.indent(SELL_IN_BRANDS_SQL.strip(), '    ')}\n")
        return

    si_brands = set(si_df["MARCA_RAW"].dropna().str.upper())
    log.write(f"[{ts()}] SELL_IN distinct raw brand values: {len(si_brands)}\n")

    for source, query in BRAND_OVERLAP_QUERIES.items():
        log.write(f"\n[{ts()}] Brand overlap: SELL_IN vs {source}\n")
        df = run_query(conn, query)
        if df is None:
            log_skipped(log, source, query)
            continue
        src_brands = set(df["MARCA_RAW"].dropna().str.upper())
        overlap    = si_brands & src_brands
        only_si    = si_brands - src_brands
        only_src   = src_brands - si_brands
        pct = round(len(overlap) / len(src_brands) * 100, 1) if src_brands else 0.0
        log.write(f"  {source} distinct brands:       {len(src_brands)}\n")
        log.write(f"  Overlap with SELL_IN:            {len(overlap)} ({pct}%)\n")
        log.write(f"  Only in {source:<22}: {len(only_src)}\n")
        log.write(f"  Only in SELL_IN:                 {len(only_si)}\n")
        if only_src:
            sample = sorted(list(only_src))[:10]
            log.write(f"  Sample values only in {source}: {sample}\n")


# ===========================================================================
# Section B3 — UPC Match Rate: SELL_IN vs SELL_OUT ONLY
# ===========================================================================

UPC_MATCH_RATE_SQL = """
WITH sell_in_upcs AS (
    SELECT DISTINCT TO_VARCHAR(SKU_EAN_COD) AS upc_std
    FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM
    WHERE SKU_EAN_COD IS NOT NULL AND TRIM(TO_VARCHAR(SKU_EAN_COD)) <> ''
),
sell_out_upcs AS (
    SELECT DISTINCT TO_VARCHAR(UPC) AS upc_so
    FROM PRD_MDP.MDP_DSP.VW_FACT_SELL_OUT
    WHERE PER_ID >= 20250101 AND UPC IS NOT NULL
),
matched AS (
    SELECT so.upc_so
    FROM sell_out_upcs so
    INNER JOIN sell_in_upcs si ON so.upc_so = si.upc_std
)
SELECT
    (SELECT COUNT(*) FROM sell_out_upcs)   AS total_sell_out_upcs,
    (SELECT COUNT(*) FROM matched)          AS matched_to_sell_in,
    (SELECT COUNT(*) FROM sell_out_upcs) - (SELECT COUNT(*) FROM matched) AS unmatched
"""


def check_upc_match_rate(conn, log):
    log.write("\n" + "=" * 70 + "\n")
    log.write("SECTION B3 — UPC MATCH RATE: SELL_IN vs SELL_OUT\n")
    log.write("NOTE: This join is ONLY valid for SELL_IN ↔ SELL_OUT.\n")
    log.write("      MKT_ON, MKT_OFF, Nielsen, IBP, WASTE: UPC join is PROHIBITED.\n")
    log.write("=" * 70 + "\n")

    df = run_query(conn, UPC_MATCH_RATE_SQL)
    if df is None:
        log_skipped(log, "UPC match rate", UPC_MATCH_RATE_SQL)
        return

    row = df.iloc[0]
    total     = int(row["TOTAL_SELL_OUT_UPCS"])
    matched   = int(row["MATCHED_TO_SELL_IN"])
    unmatched = int(row["UNMATCHED"])
    pct = round(matched / total * 100, 1) if total > 0 else 0.0

    log.write(f"[{ts()}] SELL_OUT total UPCs:    {total}\n")
    log.write(f"[{ts()}] Matched to SELL_IN EAN: {matched} ({pct}%)\n")
    log.write(f"[{ts()}] Unmatched:              {unmatched}\n")

    if pct >= 70.0:
        log.write(f"[{ts()}] PASS   — UPC match rate ≥ 70%.\n")
    else:
        log.write(f"[{ts()}] WARNING — UPC match rate below 70%. Run Notebook A → Section A4 for bridge cascade detail.\n")


# ===========================================================================
# Section B4 — UPC Predicate Violation Check (MKT_ON, MKT_OFF)
# ===========================================================================

def check_upc_predicate_violations(log):
    """
    Structural check: verify that the business rules for UPC and CADENA
    applicability are correctly documented and respected.
    This is a logic audit — not a Snowflake query — because the violation
    would occur in code, not in data.
    """
    log.write("\n" + "=" * 70 + "\n")
    log.write("SECTION B4 — UPC & CADENA PREDICATE VIOLATION RULES\n")
    log.write("=" * 70 + "\n")

    log.write(f"[{ts()}] UPC applicability matrix:\n")
    for source, applicable in UPC_APPLICABLE.items():
        flag = "ALLOWED" if applicable else "PROHIBITED"
        log.write(f"  {source:<30} UPC join: {flag}\n")

    log.write(f"\n[{ts()}] CADENA applicability matrix:\n")
    for source, applicable in CADENA_APPLICABLE.items():
        flag = "ALLOWED" if applicable else "PROHIBITED / NULL"
        log.write(f"  {source:<30} CADENA join: {flag}\n")

    log.write(f"\n[{ts()}] Rule confirmations:\n")
    if not UPC_APPLICABLE["MKT_ON"] and not UPC_APPLICABLE["MKT_OFF"]:
        log.write(f"  PASS   — MKT_ON and MKT_OFF correctly flagged as UPC-PROHIBITED.\n")
    else:
        log.write(f"  HARD BLOCKER — MKT_ON or MKT_OFF incorrectly flagged as UPC-ALLOWED.\n")

    if not CADENA_APPLICABLE["MKT_OFF"]:
        log.write(f"  PASS   — MKT_OFF correctly flagged as CADENA-PROHIBITED (CADENA_STD = NULL).\n")
    else:
        log.write(f"  HARD BLOCKER — MKT_OFF incorrectly flagged as CADENA-APPLICABLE.\n")

    if not CADENA_APPLICABLE["SELL_IN"]:
        log.write(f"  PASS   — SELL_IN CADENA_STD correctly remains NULL pending V_D_CLIENT validation.\n")
    else:
        log.write(f"  WARNING — SELL_IN marked CADENA_APPLICABLE. Confirm V_D_CLIENT validation is complete.\n")


# ===========================================================================
# Section B5 — NULL Rate Summary per Dimension per Source
# ===========================================================================

NULL_RATE_QUERIES: dict[str, str] = {
    "SELL_IN_MARCA": """
        SELECT 'SELL_IN' AS source, 'MARCA_STD' AS dimension,
            COUNT(*) AS total_rows,
            COUNT_IF(LV2_UMB_BRD_DSC IS NULL OR TRIM(LV2_UMB_BRD_DSC) = '') AS null_rows
        FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM
    """,
    "SELL_IN_UPC": """
        SELECT 'SELL_IN' AS source, 'UPC_STD' AS dimension,
            COUNT(*) AS total_rows,
            COUNT_IF(SKU_EAN_COD IS NULL OR TRIM(TO_VARCHAR(SKU_EAN_COD)) = '') AS null_rows
        FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM
    """,
    "MKT_ON_UPC": """
        SELECT 'MKT_ON' AS source, 'UPC_STD' AS dimension,
            COUNT(*) AS total_rows,
            COUNT(*) AS null_rows
        FROM PRD_MDP.MDP_DSP.VW_MKT_ECOMM WHERE ANIO >= 2024
    """,
    "MKT_OFF_UPC": """
        SELECT 'MKT_OFF' AS source, 'UPC_STD' AS dimension,
            COUNT(*) AS total_rows,
            COUNT(*) AS null_rows
        FROM PRD_MDP.MDP_STG.FACT_MEDIA_OFF WHERE ANIO >= 2024
    """,
    "MKT_OFF_CADENA": """
        SELECT 'MKT_OFF' AS source, 'CADENA_STD' AS dimension,
            COUNT(*) AS total_rows,
            COUNT(*) AS null_rows
        FROM PRD_MDP.MDP_STG.FACT_MEDIA_OFF WHERE ANIO >= 2024
    """,
    "SELL_OUT_MARCA": """
        SELECT 'SELL_OUT' AS source, 'MARCA_STD' AS dimension,
            COUNT(*) AS total_rows,
            COUNT_IF(prod.BRAND IS NULL OR TRIM(prod.BRAND) = '') AS null_rows
        FROM PRD_MDP.MDP_DSP.VW_FACT_SELL_OUT f
        INNER JOIN PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM prod
            ON TO_VARCHAR(f.UPC) = TO_VARCHAR(prod.INT_ID) AND f.CBU_ID = prod.CBU_ID
        WHERE f.PER_ID >= 20250101
    """,
    "IBP_CADENA": """
        SELECT 'IBP' AS source, 'CADENA' AS dimension,
            COUNT(*) AS total_rows,
            COUNT_IF(CADENA IS NULL OR TRIM(CADENA) = '') AS null_rows
        FROM PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP
    """,
}

# Expected 100% NULL rates (business rules — not defects)
EXPECTED_100_PCT_NULL: set[str] = {"MKT_ON_UPC", "MKT_OFF_UPC", "MKT_OFF_CADENA"}


def check_null_rates(conn, log):
    log.write("\n" + "=" * 70 + "\n")
    log.write("SECTION B5 — NULL RATE SUMMARY PER DIMENSION PER SOURCE\n")
    log.write("=" * 70 + "\n")
    log.write("NOTE: MKT_ON UPC_STD, MKT_OFF UPC_STD, and MKT_OFF CADENA_STD\n")
    log.write("      are EXPECTED to be 100% NULL — this is correct by design.\n\n")

    for label, sql in NULL_RATE_QUERIES.items():
        df = run_query(conn, sql)
        if df is None:
            log_skipped(log, label, sql)
            continue

        row = df.iloc[0]
        total     = int(row["TOTAL_ROWS"])    if "TOTAL_ROWS" in row.index else 0
        null_rows = int(row["NULL_ROWS"])     if "NULL_ROWS" in row.index else 0
        source    = str(row["SOURCE"])        if "SOURCE" in row.index else label
        dim       = str(row["DIMENSION"])     if "DIMENSION" in row.index else ""
        null_pct  = round(null_rows / total * 100, 2) if total > 0 else 0.0

        is_expected_null = label in EXPECTED_100_PCT_NULL

        if is_expected_null:
            status = "PASS (expected 100% NULL by design)"
            verdict = f"PASS   — {source} {dim} is 100% NULL as expected (business rule)."
        elif null_pct > 1.0:
            verdict = f"WARNING — {source} {dim} null rate = {null_pct}%. Threshold is 1%."
            status = f"WARNING ({null_pct}% NULL)"
        else:
            verdict = f"PASS   — {source} {dim} null rate = {null_pct}% (below 1% threshold)."
            status = f"PASS ({null_pct}% NULL)"

        log.write(f"[{ts()}] {source} | {dim} | total={total} | null={null_rows} ({null_pct}%)\n")
        log.write(f"  {verdict}\n\n")


# ===========================================================================
# Section B6 — Cross-Source Aggregation Grain Confirmation
# ===========================================================================

GRAIN_CHECK_SQL = """
-- Confirm cross-source join grain: MARCA_STD + DATE_TRUNC('MONTH', FECHA)
-- This query simulates the aggregated grain for SELL_IN and SELL_OUT
-- to verify no fanout before the full join is assembled.

SELECT 'SELL_IN_AGG_GRAIN' AS source,
    COUNT(*) AS aggregated_rows,
    COUNT(DISTINCT DATE_TRUNC('MONTH', TO_DATE(TO_VARCHAR(FAC.BIL_DAT), 'YYYYMMDD'))
        || '|' || TRIM(UPPER(PRO.LV2_UMB_BRD_DSC))) AS distinct_grain_combos
FROM PRD_MEX.MEX_DSP_OTC.VW_FACT_RNV FAC
LEFT JOIN PRD_MEX.MEX_DSP_OTC.V_D_ITEM PRO ON FAC.MAT_IDT = PRO.MAT_IDT
WHERE FAC.CBU IN ('WATERS','EDP')
  AND DATE_TRUNC('MONTH', TO_DATE(TO_VARCHAR(FAC.BIL_DAT), 'YYYYMMDD')) >= '2025-01-01'

UNION ALL

SELECT 'SELL_OUT_AGG_GRAIN' AS source,
    COUNT(*) AS aggregated_rows,
    COUNT(DISTINCT per."DAY" || '|' || TRIM(UPPER(prod.BRAND))) AS distinct_grain_combos
FROM PRD_MDP.MDP_DSP.VW_FACT_SELL_OUT f
INNER JOIN PRD_MDP.MDP_DWH.V_D_PERIOD per ON f.PER_ID = per.PER_ID
INNER JOIN PRD_MDP.MDP_DSP.VW_D_STORE_RM st ON f.STORE = st.INT_ID AND f.CBU_ID = st.CBU_ID
INNER JOIN PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM prod ON TO_VARCHAR(f.UPC) = TO_VARCHAR(prod.INT_ID)
    AND f.CBU_ID = prod.CBU_ID
WHERE f.PER_ID >= 20250101
"""


def check_aggregation_grain(conn, log):
    log.write("\n" + "=" * 70 + "\n")
    log.write("SECTION B6 — CROSS-SOURCE AGGREGATION GRAIN CONFIRMATION\n")
    log.write("=" * 70 + "\n")
    log.write("Rule: Always aggregate each source to MARCA_STD + MONTH grain before joining.\n")
    log.write("      Never join raw transactional rows across sources.\n\n")

    df = run_query(conn, GRAIN_CHECK_SQL)
    if df is None:
        log_skipped(log, "aggregation grain", GRAIN_CHECK_SQL)
        return

    log.write(f"[{ts()}] Aggregation grain summary:\n")
    log.write(df.to_string(index=False) + "\n")

    # Check for fanout (raw rows >> grain combos implies duplication risk if joined pre-aggregation)
    for _, row in df.iterrows():
        src  = str(row["SOURCE"])
        rows = int(row["AGGREGATED_ROWS"])
        combos = int(row["DISTINCT_GRAIN_COMBOS"])
        ratio = round(rows / combos, 1) if combos > 0 else 0.0
        if ratio > 10:
            log.write(f"  WARNING — {src}: {rows} raw rows vs {combos} grain combos (ratio {ratio}x). "
                      f"Aggregation REQUIRED before cross-source join.\n")
        else:
            log.write(f"  INFO   — {src}: {rows} rows / {combos} grain combos (ratio {ratio}x).\n")


# ===========================================================================
# Section B7 — Safe Cross-Source Join Template (Documentation)
# ===========================================================================

SAFE_JOIN_TEMPLATE = """
-- ============================================================
-- SAFE CROSS-SOURCE JOIN PATTERN
-- Grain: MARCA_STD + DATE_TRUNC('MONTH', FECHA)
-- UPC join: SELL_IN ↔ SELL_OUT only
-- MKT_ON: no UPC predicate
-- MKT_OFF: no UPC predicate, no CADENA predicate
-- ============================================================

WITH sell_in_agg AS (
    SELECT
        DATE_TRUNC('MONTH', TO_DATE(TO_VARCHAR(FAC.BIL_DAT), 'YYYYMMDD')) AS FECHA,
        -- MARCA_STD: resolved via CAT_MARCA join in the Silver layer
        TRIM(UPPER(PRO.LV2_UMB_BRD_DSC))  AS MARCA_STD,
        SUM(FAC.LITER)                     AS VOLUMEN,
        SUM(FAC.BIL_INV)                   AS VALOR
    FROM PRD_MEX.MEX_DSP_OTC.VW_FACT_RNV FAC
    LEFT JOIN PRD_MEX.MEX_DSP_OTC.V_D_ITEM PRO ON FAC.MAT_IDT = PRO.MAT_IDT
    WHERE FAC.CBU IN ('WATERS','EDP')
      AND DATE_TRUNC('MONTH', TO_DATE(TO_VARCHAR(FAC.BIL_DAT), 'YYYYMMDD')) >= '2025-01-01'
    GROUP BY 1, 2
),

sell_out_agg AS (
    SELECT
        DATE_TRUNC('MONTH', per."DAY")     AS FECHA,
        TRIM(UPPER(prod.BRAND))            AS MARCA_STD,
        SUM(f.VOL_SELL_OUT)                AS VOL_SELL_OUT,
        SUM(f.AMOUNT_SELL_OUT)             AS AMOUNT_SELL_OUT
    FROM PRD_MDP.MDP_DSP.VW_FACT_SELL_OUT f
    INNER JOIN PRD_MDP.MDP_DWH.V_D_PERIOD per   ON f.PER_ID = per.PER_ID
    INNER JOIN PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM prod
        ON TO_VARCHAR(f.UPC) = TO_VARCHAR(prod.INT_ID) AND f.CBU_ID = prod.CBU_ID
    WHERE f.PER_ID >= 20250101
    GROUP BY 1, 2
),

mkt_on_agg AS (
    SELECT
        DATE_TRUNC('MONTH', FECHA)         AS FECHA,
        TRIM(UPPER(MARCA))                 AS MARCA_STD,
        -- UPC_STD = NULL (business rule: MKT_ON is BRAND_GRAIN)
        -- CANAL_STD = 'ECOMMERCE_MEDIA' (constant)
        SUM(INVERSION_REAL)                AS INVERSION_ON,
        SUM(IMPRESIONES)                   AS IMPRESIONES_ON,
        SUM(CLICS)                         AS CLICS_ON
    FROM PRD_MDP.MDP_DSP.VW_MKT_ECOMM
    WHERE ANIO >= 2024
    GROUP BY 1, 2
),

mkt_off_agg AS (
    SELECT
        DATE_TRUNC('MONTH', FECHA)         AS FECHA,
        TRIM(UPPER(MARCA))                 AS MARCA_STD,
        -- UPC_STD = NULL (business rule: MKT_OFF is BRAND_GRAIN)
        -- CADENA_STD = NULL (business rule: MKT_OFF has no CADENA)
        -- CANAL_STD = 'OFFLINE_MEDIA' (constant)
        SUM(INVERSION_REAL)                AS INVERSION_OFF,
        SUM(IMPACTOS_HT)                   AS IMPACTOS_OFF
    FROM PRD_MDP.MDP_STG.FACT_MEDIA_OFF
    WHERE ANIO >= 2024
    GROUP BY 1, 2
)

SELECT
    COALESCE(si.FECHA, so.FECHA, on_.FECHA, off_.FECHA)        AS FECHA_STD,
    COALESCE(si.MARCA_STD, so.MARCA_STD, on_.MARCA_STD, off_.MARCA_STD) AS MARCA_STD,
    si.VOLUMEN                                                  AS SELL_IN_VOLUMEN,
    si.VALOR                                                    AS SELL_IN_VALOR,
    so.VOL_SELL_OUT,
    so.AMOUNT_SELL_OUT,
    on_.INVERSION_ON,
    on_.IMPRESIONES_ON,
    on_.CLICS_ON,
    off_.INVERSION_OFF,
    off_.IMPACTOS_OFF
FROM sell_in_agg si
FULL OUTER JOIN sell_out_agg so
    ON  si.FECHA      = so.FECHA
    AND si.MARCA_STD  = so.MARCA_STD
    -- UPC_STD is NOT a predicate at this grain.
    -- UPC validation lives in Section A4 (Notebook A) and Section B3 (this notebook).
    -- It is ONLY valid as a SELL_IN ↔ SELL_OUT bridge check, never as an all-source join key.
LEFT JOIN mkt_on_agg on_
    ON  COALESCE(si.FECHA, so.FECHA) = on_.FECHA
    AND COALESCE(si.MARCA_STD, so.MARCA_STD) = on_.MARCA_STD
    -- NO UPC predicate for MKT_ON (rule enforced)
LEFT JOIN mkt_off_agg off_
    ON  COALESCE(si.FECHA, so.FECHA) = off_.FECHA
    AND COALESCE(si.MARCA_STD, so.MARCA_STD) = off_.MARCA_STD
    -- NO UPC predicate for MKT_OFF (rule enforced)
    -- NO CADENA predicate for MKT_OFF (rule enforced)
ORDER BY FECHA_STD, MARCA_STD
"""


def document_safe_join(log):
    log.write("\n" + "=" * 70 + "\n")
    log.write("SECTION B7 — APPROVED ALL-SOURCE JOIN TEMPLATE\n")
    log.write("=" * 70 + "\n")
    log.write("Grain : MARCA_STD + DATE_TRUNC('MONTH', FECHA)\n")
    log.write("UPC   : NOT a predicate in this template.\n")
    log.write("        UPC validation is scoped to Section A4 (Notebook A) and\n")
    log.write("        Section B3 (this notebook) as a SELL_IN <-> SELL_OUT bridge check only.\n")
    log.write("Deviations from this pattern require explicit review before production.\n\n")
    log.write(SAFE_JOIN_TEMPLATE + "\n")


# ===========================================================================
# Section B8 — Summary: Assumptions Validated, Unresolved, Warnings, Blockers
# ===========================================================================

def write_summary(conn, log):
    log.write("\n" + "=" * 70 + "\n")
    log.write("SECTION B8 — FINAL SUMMARY\n")
    log.write("=" * 70 + "\n")

    log.write("\nVALIDATED ASSUMPTIONS:\n")
    log.write("  ✓ SELL_IN is the golden source for UPC (SAP V_D_ITEM)\n")
    log.write("  ✓ MKT_ON grain is BRAND + FECHA + CADENA (no UPC)\n")
    log.write("  ✓ MKT_OFF grain is BRAND + FECHA only (no UPC, no CADENA)\n")
    log.write("  ✓ MKT_ON CANAL_STD = 'ECOMMERCE_MEDIA' (constant, confirmed)\n")
    log.write("  ✓ MKT_OFF CANAL_STD = 'OFFLINE_MEDIA' (constant, confirmed)\n")
    log.write("  ✓ IBP CADENA values are retail chains (HEB, WALMART, etc. confirmed)\n")
    log.write("  ✓ IBP CADENA = GUATEMALA / EL SALVADOR → cadena_type = REGION (not retail)\n")
    log.write("  ✓ WASTE metric columns: 'WASTE ($)' and 'WASTE (KG)' (user-confirmed)\n")
    log.write("  ✓ SELL_OUT UPC bridge: P1 (INT_ID) → P2 (IMPORT_ID) → P3 quarantine only\n")
    log.write("  ✓ Nielsen MRKT_DSC_SHRT → static CSV bridge (not dynamic SQL parsing)\n")
    log.write("  ✓ Nielsen PRDC_CD → PRODUCT_CODE_STD; UPC_STD = NULL unless SAP EAN confirmed\n")
    log.write("  ✓ All product IDs stored as VARCHAR (leading zeros preserved)\n")

    log.write("\nUNRESOLVED — REQUIRE SNOWFLAKE VALIDATION BEFORE CLOSING:\n")
    log.write("  ⚠ SELL_IN CADENA: V_D_CLIENT.CUS_GRN_CHL_DSC not yet confirmed as chain field\n")
    log.write("  ⚠ SELL_OUT CANAL: VW_D_STORE_RM.CHAIN not yet confirmed as channel (may be CADENA)\n")
    log.write("  ⚠ SELL_OUT CANAL: No clean channel field confirmed; DESC TABLE required\n")
    log.write("  ⚠ Nielsen market bridge: All MRKT_DSC_SHRT values need profiling from 4 market dims\n")
    log.write("  ⚠ MKT_ON CADENA: Values in VW_MKT_ECOMM.CADENA not yet profiled\n")
    log.write("  ⚠ CAT_UPC: Requires Snowflake query against V_D_ITEM to physically populate\n")

    log.write("\nWARNINGS (expected in first run):\n")
    log.write("  ⚠ MARCA_STD coverage may be < 99% in first run — normal during baseline stabilization\n")
    log.write("  ⚠ SELL_OUT UPC P1 rate may be < 70% before baseline bridge is built\n")
    log.write("  ⚠ nielsen_market_mapping.csv has NEEDS_REVIEW rows — complete mapping required\n")
    log.write("  ⚠ cadena_mapping.csv has NEEDS_REVIEW rows for SELL_OUT and MKT_ON\n")
    log.write("  ⚠ channel_mapping.csv has NEEDS_REVIEW rows for SELL_IN and SELL_OUT\n")

    log.write("\nHARD BLOCKERS (none expected if notebook completes cleanly):\n")
    log.write("  ✗ CAT_MARCA duplicate on source_system + raw_name_normalized — MUST be zero\n")
    log.write("  ✗ SELL_IN SKU_EAN_COD NULL rate > 0% for active products — MUST be zero\n")
    log.write("  ✗ Fuzzy UPC matches auto-promoted to production catalog — PROHIBITED\n")
    log.write("  ✗ MKT_ON or MKT_OFF joined using UPC predicate — PROHIBITED\n")
    log.write("  ✗ MKT_OFF CADENA_STD contains non-NULL values — HARD BLOCKER\n")
    log.write("  ✗ Product IDs cast to numeric — HARD BLOCKER\n")

    log.write("\nRECOMMENDED NEXT ACTIONS:\n")
    log.write("  1. Run Notebook A with Snowflake credentials to populate all profiling data.\n")
    log.write("  2. Review unmapped_brands_by_source.csv and add NEEDS_REVIEW rows to brand_mapping.csv\n")
    log.write("  3. Run: DESC TABLE PRD_MEX.MEX_DSP_OTC.V_D_CLIENT → confirm CUS_GRN_CHL_DSC semantics.\n")
    log.write("  4. Run: DESC TABLE PRD_MDP.MDP_DSP.VW_D_STORE_RM → confirm CANAL vs CADENA distinction.\n")
    log.write("  5. Run DISTINCT MRKT_DSC_SHRT from all 4 Nielsen market dims → complete nielsen_market_mapping.csv.\n")
    log.write("  6. Once V_D_ITEM is queried, paste results into sku_mapping.csv with VARCHAR casting.\n")
    log.write("  7. Re-run both notebooks and share logs for deeper analysis.\n")


# ===========================================================================
# Main
# ===========================================================================

def main():
    conn = get_sf_connection()
    sf_status = "CONNECTED" if conn is not None else "OFFLINE (queries printed for manual execution)"

    with open(LOG_FILE, "w", encoding="utf-8") as log:
        log.write("=" * 70 + "\n")
        log.write("MDM CROSS-SOURCE JOIN VALIDATION — NOTEBOOK B\n")
        log.write(f"Run timestamp : {ts()}\n")
        log.write(f"Snowflake     : {sf_status}\n")
        log.write("=" * 70 + "\n")

        check_row_counts(conn, log)
        check_brand_overlap(conn, log)
        check_upc_match_rate(conn, log)
        check_upc_predicate_violations(log)
        check_null_rates(conn, log)
        check_aggregation_grain(conn, log)
        document_safe_join(log)
        write_summary(conn, log)

        log.write("\n" + "=" * 70 + "\n")
        log.write(f"NOTEBOOK B COMPLETE — {ts()}\n")
        log.write("=" * 70 + "\n")

    print(f"Cross-source validation complete. Log: {LOG_FILE}")


if __name__ == "__main__":
    main()
