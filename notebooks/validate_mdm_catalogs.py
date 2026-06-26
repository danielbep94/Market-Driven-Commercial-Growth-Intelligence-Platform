"""
MDM Catalog Validation Notebook A
===================================
Purpose : Profile the four MDM catalogs (MARCA, UPC, CANAL, CADENA) against
          all source systems and write structured audit logs.

Golden source for UPC  : PRD_MEX.MEX_DSP_OTC.V_D_ITEM  (SELL_IN / SAP)
Fuzzy match policy     : Quarantine ONLY — never auto-insert into production.
Leading zeros policy   : All product IDs must remain VARCHAR throughout.

Output files
------------
logs/validation_results_mdm_catalogs.txt   — master audit log (human-readable)
logs/unmapped_brands_by_source.csv         — brand values with no MARCA_STD
logs/unmapped_channels_by_source.csv       — channel values with no CANAL_STD
logs/unmapped_cadenas_by_source.csv        — cadena values with no CADENA_STD
logs/upc_bridge_unmatched_sell_out.csv     — SELL_OUT UPCs that found no match
logs/upc_bridge_fuzzy_candidates.csv       — P3 fuzzy matches (review required)

Prerequisites
-------------
pip install snowflake-connector-python pandas thefuzz python-levenshtein
Environment variables required:
    SF_ACCOUNT, SF_USER, SF_PASSWORD (or SF_PRIVATE_KEY), SF_ROLE,
    SF_WAREHOUSE, SF_DATABASE_PRD_MEX, SF_DATABASE_PRD_MDP

Run locally:
    python notebooks/validate_mdm_catalogs.py
"""

import os
import csv
import datetime
import textwrap
import pandas as pd

# ---------------------------------------------------------------------------
# Optional fuzzy import — only used for P3 UPC candidates (quarantine only)
# ---------------------------------------------------------------------------
try:
    from thefuzz import fuzz
    FUZZY_AVAILABLE = True
except ImportError:
    FUZZY_AVAILABLE = False
    print("WARNING: thefuzz not installed — P3 fuzzy UPC matching will be skipped.")

# ---------------------------------------------------------------------------
# Snowflake connector
# ---------------------------------------------------------------------------
try:
    import snowflake.connector
    SF_AVAILABLE = True
except ImportError:
    SF_AVAILABLE = False
    print("WARNING: snowflake-connector-python not installed — all queries will be skipped.")


# ===========================================================================
# Configuration
# ===========================================================================
BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HOMO_DIR    = os.path.join(BASE_DIR, "homologation")
LOGS_DIR    = os.path.join(BASE_DIR, "logs")
os.makedirs(LOGS_DIR, exist_ok=True)

CATALOG_FILES = {
    "brand":   os.path.join(HOMO_DIR, "brand_mapping.csv"),
    "sku":     os.path.join(HOMO_DIR, "sku_mapping.csv"),
    "channel": os.path.join(HOMO_DIR, "channel_mapping.csv"),
    "cadena":  os.path.join(HOMO_DIR, "cadena_mapping.csv"),
    "nielsen": os.path.join(HOMO_DIR, "nielsen_market_mapping.csv"),
}

LOG_FILE              = os.path.join(LOGS_DIR, "validation_results_mdm_catalogs.txt")
UNMAPPED_BRANDS_CSV   = os.path.join(LOGS_DIR, "unmapped_brands_by_source.csv")
UNMAPPED_CHANNELS_CSV = os.path.join(LOGS_DIR, "unmapped_channels_by_source.csv")
UNMAPPED_CADENAS_CSV  = os.path.join(LOGS_DIR, "unmapped_cadenas_by_source.csv")
UPC_UNMATCHED_CSV     = os.path.join(LOGS_DIR, "upc_bridge_unmatched_sell_out.csv")
UPC_FUZZY_CSV         = os.path.join(LOGS_DIR, "upc_bridge_fuzzy_candidates.csv")

# Source systems defined in semantic layer
SOURCE_SYSTEMS = [
    "SELL_IN", "SELL_OUT", "MKT_ON", "MKT_OFF",
    "EDP_NIELSEN", "PB_NIELSEN", "WATER_NIELSEN_RIE",
    "WATER_SCANTRACK", "IBP", "WASTE",
]

# UPC applicability by source (business rule)
UPC_APPLICABLE = {
    "SELL_IN": True,
    "SELL_OUT": True,       # after bridge cascade
    "MKT_ON": False,        # BRAND_GRAIN — no UPC
    "MKT_OFF": False,       # BRAND_GRAIN — no UPC
    "EDP_NIELSEN": False,   # NIELSEN_PRDC_CD only, not SAP EAN
    "PB_NIELSEN": False,    # NIELSEN_PRDC_CD only
    "WATER_NIELSEN_RIE": False,   # h=9, no UPC level
    "WATER_SCANTRACK": False,     # PRDC_CD at h=11 only, not confirmed SAP
    "IBP": False,           # text SKU — candidates only
    "WASTE": False,         # text SKU — candidates only
}

# CADENA applicability by source
CADENA_APPLICABLE = {
    "SELL_IN": False,       # NULL until V_D_CLIENT confirmed
    "SELL_OUT": True,
    "MKT_ON": True,
    "MKT_OFF": False,       # explicitly absent — CADENA_STD = NULL
    "EDP_NIELSEN": True,    # via CAT_MERCADO_NIELSEN
    "PB_NIELSEN": True,
    "WATER_NIELSEN_RIE": True,
    "WATER_SCANTRACK": True,
    "IBP": True,
    "WASTE": True,
}


# ===========================================================================
# Utility helpers
# ===========================================================================

def ts() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_sf_connection():
    """Return a Snowflake connection using environment variables."""
    if not SF_AVAILABLE:
        return None
    required = ["SF_ACCOUNT", "SF_USER", "SF_PASSWORD", "SF_WAREHOUSE"]
    missing  = [k for k in required if not os.getenv(k)]
    if missing:
        return None
    try:
        conn = snowflake.connector.connect(
            account   = os.environ["SF_ACCOUNT"],
            user      = os.environ["SF_USER"],
            password  = os.environ["SF_PASSWORD"],
            warehouse = os.environ["SF_WAREHOUSE"],
            role      = os.getenv("SF_ROLE", ""),
        )
        return conn
    except Exception as exc:
        return None


def run_query(conn, sql: str, label: str) -> pd.DataFrame | None:
    """Execute SQL and return a DataFrame, or None on error."""
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        return pd.DataFrame(rows, columns=cols)
    except Exception as exc:
        return None


def load_csv(path: str) -> pd.DataFrame:
    """Load a catalog CSV; return empty DataFrame if absent or scaffold."""
    if not os.path.exists(path):
        return pd.DataFrame()
    df = pd.read_csv(path, dtype=str).fillna("")
    # Drop scaffold placeholder rows
    df = df[~df.iloc[:, 0].str.startswith("[PENDING")]
    df = df[~df.iloc[:, 0].str.startswith("[TO_BE")]
    return df


def write_csv(path: str, rows: list[dict], fieldnames: list[str]):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


# ===========================================================================
# Section A1 — CAT_MARCA Structural Checks
# ===========================================================================

def check_marca_catalog(log):
    log.write("\n" + "=" * 70 + "\n")
    log.write("SECTION A1 — CAT_MARCA STRUCTURAL VALIDATION\n")
    log.write("=" * 70 + "\n")

    df = load_csv(CATALOG_FILES["brand"])
    if df.empty:
        log.write(f"[{ts()}] ERROR  — brand_mapping.csv is empty or missing.\n")
        log.write("  SEVERITY: HARD BLOCKER — CAT_MARCA must exist before any source join.\n")
        return df

    total = len(df)
    log.write(f"[{ts()}] INFO   — Loaded {total} rows from brand_mapping.csv\n")

    # Check 1: Uniqueness on source_system + raw_name_normalized
    if "source_system" in df.columns and "raw_name_normalized" in df.columns:
        dupes = (
            df.groupby(["source_system", "raw_name_normalized"])
            .size()
            .reset_index(name="count")
        )
        dupes = dupes[dupes["count"] > 1]
        if len(dupes) > 0:
            log.write(f"[{ts()}] HARD BLOCKER — Duplicate source_system+raw_name_normalized found:\n")
            log.write(dupes.to_string(index=False) + "\n")
        else:
            log.write(f"[{ts()}] PASS   — Uniqueness check passed: no duplicate source+raw_name combinations.\n")

    # Check 2: mapping_status distribution
    if "mapping_status" in df.columns:
        status_counts = df["mapping_status"].value_counts()
        log.write(f"[{ts()}] INFO   — mapping_status distribution:\n")
        for status, cnt in status_counts.items():
            log.write(f"           {status}: {cnt}\n")

    # Check 3: CONFIRMED coverage per source
    if "mapping_status" in df.columns and "source_system" in df.columns:
        confirmed = df[df["mapping_status"] == "CONFIRMED"]
        needs_review = df[df["mapping_status"] == "NEEDS_REVIEW"]
        log.write(f"[{ts()}] INFO   — CONFIRMED entries: {len(confirmed)}\n")
        log.write(f"[{ts()}] INFO   — NEEDS_REVIEW entries: {len(needs_review)}\n")
        if len(needs_review) > 0:
            log.write(f"[{ts()}] WARNING — {len(needs_review)} brands need review. See unmapped_brands_by_source.csv\n")

    # Check 4: Sources with ZERO confirmed entries
    covered = set(df[df["mapping_status"] == "CONFIRMED"]["source_system"].unique()) if "source_system" in df.columns else set()
    missing_sources = [s for s in SOURCE_SYSTEMS if s not in covered]
    if missing_sources:
        log.write(f"[{ts()}] WARNING — Sources with NO CONFIRMED brand mappings: {missing_sources}\n")
        log.write(f"           These sources will produce NEEDS_REVIEW entries in Notebook A2 profiling.\n")
    else:
        log.write(f"[{ts()}] PASS   — All source systems have at least one CONFIRMED brand mapping.\n")

    return df


# ===========================================================================
# Section A2 — Brand Coverage Profiling (Snowflake Queries)
# ===========================================================================

BRAND_PROFILE_QUERIES: dict[str, str] = {
    "SELL_IN": """
        SELECT DISTINCT
            'SELL_IN'                       AS source_system,
            TRIM(UPPER(LV2_UMB_BRD_DSC))   AS raw_name_normalized,
            COUNT(DISTINCT MAT_IDT)         AS distinct_skus
        FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM
        WHERE LV2_UMB_BRD_DSC IS NOT NULL
        GROUP BY TRIM(UPPER(LV2_UMB_BRD_DSC))
        ORDER BY distinct_skus DESC
    """,
    "SELL_OUT": """
        SELECT DISTINCT
            'SELL_OUT'                      AS source_system,
            TRIM(UPPER(prod.BRAND))         AS raw_name_normalized,
            COUNT(DISTINCT TO_VARCHAR(f.UPC)) AS distinct_upcs
        FROM PRD_MDP.MDP_DSP.VW_FACT_SELL_OUT f
        INNER JOIN PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM prod
            ON TO_VARCHAR(f.UPC) = TO_VARCHAR(prod.INT_ID)
           AND f.CBU_ID = prod.CBU_ID
        WHERE prod.BRAND IS NOT NULL
        GROUP BY TRIM(UPPER(prod.BRAND))
        ORDER BY distinct_upcs DESC
    """,
    "MKT_ON": """
        SELECT DISTINCT
            'MKT_ON'                        AS source_system,
            TRIM(UPPER(MARCA))              AS raw_name_normalized,
            COUNT(*)                        AS row_count
        FROM PRD_MDP.MDP_DSP.VW_MKT_ECOMM
        WHERE MARCA IS NOT NULL
        GROUP BY TRIM(UPPER(MARCA))
        ORDER BY row_count DESC
    """,
    "MKT_OFF": """
        SELECT DISTINCT
            'MKT_OFF'                       AS source_system,
            TRIM(UPPER(MARCA))              AS raw_name_normalized,
            COUNT(*)                        AS row_count
        FROM PRD_MDP.MDP_STG.FACT_MEDIA_OFF
        WHERE MARCA IS NOT NULL
        GROUP BY TRIM(UPPER(MARCA))
        ORDER BY row_count DESC
    """,
    "IBP": """
        SELECT DISTINCT
            'IBP'                           AS source_system,
            TRIM(UPPER(MARCA))              AS raw_name_normalized,
            COUNT(*)                        AS row_count
        FROM PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP
        WHERE MARCA IS NOT NULL
        GROUP BY TRIM(UPPER(MARCA))
        ORDER BY row_count DESC
    """,
    "WASTE": """
        SELECT DISTINCT
            'WASTE'                         AS source_system,
            TRIM(UPPER(MARCA))              AS raw_name_normalized,
            COUNT(*)                        AS row_count
        FROM PRD_MDP.MDP_STG.FACT_TOPLINE
        WHERE MARCA IS NOT NULL
          AND UPPER(TRIM(FUENTE)) = 'TOPLINE'
        GROUP BY TRIM(UPPER(MARCA))
        ORDER BY row_count DESC
    """,
    "EDP_NIELSEN": """
        SELECT DISTINCT
            'EDP_NIELSEN'                   AS source_system,
            TRIM(UPPER(INP_56985))          AS raw_name_normalized,
            COUNT(DISTINCT product_id)      AS distinct_products
        FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_PROD_DIM
        WHERE INP_56985 IS NOT NULL
        GROUP BY TRIM(UPPER(INP_56985))
        ORDER BY distinct_products DESC
    """,
    "PB_NIELSEN": """
        SELECT DISTINCT
            'PB_NIELSEN'                    AS source_system,
            TRIM(UPPER(INP_56985))          AS raw_name_normalized,
            COUNT(DISTINCT product_id)      AS distinct_products
        FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_SUST_LECHE_ST_NLSN_PROD_DIM
        WHERE INP_56985 IS NOT NULL
        GROUP BY TRIM(UPPER(INP_56985))
        ORDER BY distinct_products DESC
    """,
    "WATER_NIELSEN_RIE": """
        SELECT DISTINCT
            'WATER_NIELSEN_RIE'             AS source_system,
            TRIM(UPPER(CSTM_310589))        AS raw_name_normalized,
            COUNT(DISTINCT product_id)      AS distinct_products
        FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_RT_NLSN_PROD_DIM
        WHERE CSTM_310589 IS NOT NULL
        GROUP BY TRIM(UPPER(CSTM_310589))
        ORDER BY distinct_products DESC
    """,
    "WATER_SCANTRACK": """
        SELECT DISTINCT
            'WATER_SCANTRACK'               AS source_system,
            TRIM(UPPER(CSTM_310589))        AS raw_name_normalized,
            COUNT(DISTINCT product_id)      AS distinct_products
        FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_ST_NLSN_PROD_DIM
        WHERE CSTM_310589 IS NOT NULL
        GROUP BY TRIM(UPPER(CSTM_310589))
        ORDER BY distinct_products DESC
    """,
}


def check_brand_coverage(conn, brand_df: pd.DataFrame, log):
    log.write("\n" + "=" * 70 + "\n")
    log.write("SECTION A2 — BRAND COVERAGE PROFILING PER SOURCE\n")
    log.write("=" * 70 + "\n")

    confirmed_set = set()
    if not brand_df.empty and "raw_name_normalized" in brand_df.columns:
        confirmed_set = set(
            brand_df[brand_df.get("mapping_status", pd.Series(dtype=str)) == "CONFIRMED"]["raw_name_normalized"].str.upper()
        )

    unmapped_rows = []

    for source, query in BRAND_PROFILE_QUERIES.items():
        log.write(f"\n[{ts()}] Profiling brand values for source: {source}\n")
        df = run_query(conn, query, source)

        if df is None:
            log.write(f"  SKIPPED — Snowflake connection unavailable or query failed.\n")
            log.write(f"  SQL to run manually:\n{textwrap.indent(query.strip(), '    ')}\n")
            continue

        total = len(df)
        mapped = 0
        for _, row in df.iterrows():
            norm = str(row.get("raw_name_normalized", "")).strip().upper()
            if norm in confirmed_set:
                mapped += 1
            else:
                unmapped_rows.append({
                    "source_system":        source,
                    "raw_name_normalized":  norm,
                    "row_count_or_products": row.iloc[2] if len(row) > 2 else "",
                    "recommended_action":   "Add to brand_mapping.csv with mapping_status=NEEDS_REVIEW",
                })

        pct = round(mapped / total * 100, 1) if total > 0 else 0.0
        log.write(f"  Total distinct brand values: {total}\n")
        log.write(f"  Mapped to CAT_MARCA:         {mapped} ({pct}%)\n")
        log.write(f"  Unmapped:                    {total - mapped}\n")
        if pct < 99.0:
            log.write(f"  WARNING — Coverage below 99%. Review unmapped_brands_by_source.csv\n")
        else:
            log.write(f"  PASS — Coverage meets threshold.\n")

    write_csv(
        UNMAPPED_BRANDS_CSV,
        unmapped_rows,
        ["source_system", "raw_name_normalized", "row_count_or_products", "recommended_action"],
    )
    log.write(f"\n[{ts()}] Exported {len(unmapped_rows)} unmapped brand rows → {UNMAPPED_BRANDS_CSV}\n")


# ===========================================================================
# Section A3 — UPC Golden Source Checks (V_D_ITEM)
# ===========================================================================

VDITEM_NULL_CHECK_SQL = """
SELECT
    COUNT(*)                          AS total_rows,
    COUNT_IF(SKU_EAN_COD IS NULL)     AS null_ean_rows,
    COUNT_IF(TRIM(SKU_EAN_COD) = '')  AS blank_ean_rows,
    COUNT(DISTINCT TO_VARCHAR(SKU_EAN_COD)) AS distinct_ean,
    COUNT(DISTINCT TO_VARCHAR(MAT_IDT))     AS distinct_mat_idt
FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM
"""

VDITEM_CARDINALITY_SQL = """
SELECT
    TO_VARCHAR(SKU_EAN_COD)          AS sku_ean_cod,
    COUNT(DISTINCT TO_VARCHAR(MAT_IDT)) AS distinct_mat_idt,
    COUNT(*)                         AS row_count
FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM
WHERE SKU_EAN_COD IS NOT NULL
  AND TRIM(TO_VARCHAR(SKU_EAN_COD)) <> ''
GROUP BY TO_VARCHAR(SKU_EAN_COD)
HAVING COUNT(DISTINCT TO_VARCHAR(MAT_IDT)) > 1
ORDER BY distinct_mat_idt DESC, row_count DESC
LIMIT 50
"""


def check_upc_golden_source(conn, log):
    log.write("\n" + "=" * 70 + "\n")
    log.write("SECTION A3 — UPC GOLDEN SOURCE QUALITY (V_D_ITEM)\n")
    log.write("=" * 70 + "\n")

    # Check A3.1 — Null / blank EAN coverage
    log.write(f"[{ts()}] Running null/blank EAN check on V_D_ITEM...\n")
    df_null = run_query(conn, VDITEM_NULL_CHECK_SQL, "V_D_ITEM_NULL")
    if df_null is None:
        log.write(f"  SKIPPED — Snowflake unavailable.\n")
        log.write(f"  SQL to run manually:\n{textwrap.indent(VDITEM_NULL_CHECK_SQL.strip(), '    ')}\n")
    else:
        row = df_null.iloc[0]
        total       = int(row["TOTAL_ROWS"])
        null_ean    = int(row["NULL_EAN_ROWS"])
        blank_ean   = int(row["BLANK_EAN_ROWS"])
        distinct_ean = int(row["DISTINCT_EAN"])
        distinct_mat = int(row["DISTINCT_MAT_IDT"])
        null_pct    = round(null_ean / total * 100, 2) if total > 0 else 0.0

        log.write(f"  Total V_D_ITEM rows:    {total}\n")
        log.write(f"  NULL SKU_EAN_COD:       {null_ean} ({null_pct}%)\n")
        log.write(f"  Blank SKU_EAN_COD:      {blank_ean}\n")
        log.write(f"  Distinct EAN values:    {distinct_ean}\n")
        log.write(f"  Distinct MAT_IDT:       {distinct_mat}\n")

        if null_ean > 0:
            log.write(f"  WARNING — {null_ean} rows have NULL EAN. These SKUs cannot serve as golden UPC source.\n")
        else:
            log.write(f"  PASS — No NULL EAN rows.\n")

    # Check A3.2 — EAN-to-MAT_IDT cardinality
    log.write(f"\n[{ts()}] Running EAN cardinality check (1 EAN → multiple MAT_IDT)...\n")
    df_card = run_query(conn, VDITEM_CARDINALITY_SQL, "V_D_ITEM_CARD")
    if df_card is None:
        log.write(f"  SKIPPED — Snowflake unavailable.\n")
        log.write(f"  SQL to run manually:\n{textwrap.indent(VDITEM_CARDINALITY_SQL.strip(), '    ')}\n")
    else:
        if len(df_card) == 0:
            log.write(f"  PASS — All EAN codes map to exactly one MAT_IDT.\n")
        else:
            log.write(f"  WARNING — {len(df_card)} EAN codes map to multiple MAT_IDTs (top 50 shown):\n")
            log.write(df_card.to_string(index=False) + "\n")
            log.write(f"  These must be reviewed before UPC catalog is used as golden source.\n")


# ===========================================================================
# Section A4 — SELL_OUT UPC Bridge Cascade
# ===========================================================================

UPC_BRIDGE_P1_SQL = """
SELECT
    TO_VARCHAR(prod.INT_ID)         AS sell_out_int_id,
    TO_VARCHAR(item.SKU_EAN_COD)    AS sku_ean_cod,
    TO_VARCHAR(item.MAT_IDT)        AS mat_idt,
    item.MAT_LCL_DSC,
    prod.NAME                       AS sell_out_name,
    prod.BRAND                      AS sell_out_brand,
    prod.CBU_ID,
    1                               AS match_priority,
    'EXACT_INT_ID'                  AS match_method,
    1.0                             AS match_confidence
FROM PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM prod
INNER JOIN PRD_MEX.MEX_DSP_OTC.V_D_ITEM item
    ON TO_VARCHAR(prod.INT_ID) = TO_VARCHAR(item.SKU_EAN_COD)
WHERE prod.INT_ID IS NOT NULL
"""

UPC_BRIDGE_P2_SQL = """
SELECT
    TO_VARCHAR(prod.IMPORT_ID)      AS sell_out_import_id,
    TO_VARCHAR(item.SKU_EAN_COD)    AS sku_ean_cod,
    TO_VARCHAR(item.MAT_IDT)        AS mat_idt,
    item.MAT_LCL_DSC,
    prod.NAME                       AS sell_out_name,
    prod.BRAND                      AS sell_out_brand,
    prod.CBU_ID,
    2                               AS match_priority,
    'EXACT_IMPORT_ID'               AS match_method,
    1.0                             AS match_confidence
FROM PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM prod
INNER JOIN PRD_MEX.MEX_DSP_OTC.V_D_ITEM item
    ON TO_VARCHAR(prod.IMPORT_ID) = TO_VARCHAR(item.SKU_EAN_COD)
LEFT JOIN (
    SELECT DISTINCT TO_VARCHAR(INT_ID) AS already_matched_int_id
    FROM PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM p2
    INNER JOIN PRD_MEX.MEX_DSP_OTC.V_D_ITEM i2
        ON TO_VARCHAR(p2.INT_ID) = TO_VARCHAR(i2.SKU_EAN_COD)
) matched_p1
    ON TO_VARCHAR(prod.IMPORT_ID) = matched_p1.already_matched_int_id
WHERE prod.IMPORT_ID IS NOT NULL
  AND matched_p1.already_matched_int_id IS NULL
"""

SELL_OUT_UNMATCHED_SQL = """
SELECT
    TO_VARCHAR(prod.INT_ID)     AS int_id,
    TO_VARCHAR(prod.IMPORT_ID)  AS import_id,
    prod.NAME                   AS sell_out_name,
    prod.BRAND                  AS sell_out_brand,
    prod.CBU_ID
FROM PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM prod
WHERE NOT EXISTS (
    SELECT 1 FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM item
    WHERE TO_VARCHAR(prod.INT_ID) = TO_VARCHAR(item.SKU_EAN_COD)
)
AND NOT EXISTS (
    SELECT 1 FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM item
    WHERE TO_VARCHAR(prod.IMPORT_ID) = TO_VARCHAR(item.SKU_EAN_COD)
)
ORDER BY prod.BRAND, prod.NAME
"""


def check_upc_bridge(conn, log):
    log.write("\n" + "=" * 70 + "\n")
    log.write("SECTION A4 — SELL_OUT UPC BRIDGE CASCADE VALIDATION\n")
    log.write("=" * 70 + "\n")

    p1_df = run_query(conn, UPC_BRIDGE_P1_SQL, "UPC_BRIDGE_P1")
    p2_df = run_query(conn, UPC_BRIDGE_P2_SQL, "UPC_BRIDGE_P2")
    unmatched_df = run_query(conn, SELL_OUT_UNMATCHED_SQL, "UPC_UNMATCHED")

    if p1_df is None:
        log.write(f"[{ts()}] SKIPPED — Snowflake unavailable. Paste queries manually.\n")
        for label, sql in [("P1 SQL", UPC_BRIDGE_P1_SQL), ("P2 SQL", UPC_BRIDGE_P2_SQL),
                            ("UNMATCHED SQL", SELL_OUT_UNMATCHED_SQL)]:
            log.write(f"  {label}:\n{textwrap.indent(sql.strip(), '    ')}\n\n")
        return

    p1_cnt       = len(p1_df)     if p1_df is not None else 0
    p2_cnt       = len(p2_df)     if p2_df is not None else 0
    unmatched_cnt = len(unmatched_df) if unmatched_df is not None else 0
    total_so = p1_cnt + p2_cnt + unmatched_cnt

    log.write(f"[{ts()}] UPC Bridge results:\n")
    log.write(f"  Priority 1 (INT_ID exact):     {p1_cnt}\n")
    log.write(f"  Priority 2 (IMPORT_ID exact):  {p2_cnt}\n")
    log.write(f"  Unmatched (no exact match):    {unmatched_cnt}\n")
    log.write(f"  Total SELL_OUT products:       {total_so}\n")

    if total_so > 0:
        p1_pct = round(p1_cnt / total_so * 100, 1)
        log.write(f"  Priority-1 match rate: {p1_pct}%\n")
        if p1_pct < 70.0:
            log.write(f"  WARNING — P1 match rate below 70% threshold. Investigate SELL_OUT product master alignment.\n")
        else:
            log.write(f"  PASS — P1 match rate meets 70% threshold.\n")

    # P3 fuzzy (quarantine only)
    fuzzy_rows = []
    if unmatched_df is not None and len(unmatched_df) > 0 and FUZZY_AVAILABLE:
        si_df = run_query(conn, """
            SELECT TO_VARCHAR(MAT_IDT) AS mat_idt, MAT_LCL_DSC, TRIM(UPPER(LV2_UMB_BRD_DSC)) AS marca
            FROM PRD_MEX.MEX_DSP_OTC.V_D_ITEM WHERE MAT_ACT_FLG = 1
        """, "SELL_IN_PRODUCTS")

        if si_df is not None:
            for _, so_row in unmatched_df.iterrows():
                best_score = 0
                best_match = None
                so_name = str(so_row.get("SELL_OUT_NAME", "")).upper()
                so_brand = str(so_row.get("SELL_OUT_BRAND", "")).upper()
                for _, si_row in si_df.iterrows():
                    si_name = str(si_row.get("MAT_LCL_DSC", "")).upper()
                    si_brand = str(si_row.get("MARCA", "")).upper()
                    score = (fuzz.token_sort_ratio(so_name, si_name) * 0.7 +
                             fuzz.ratio(so_brand, si_brand) * 0.3)
                    if score > best_score:
                        best_score = score
                        best_match = si_row
                if best_score >= 70 and best_match is not None:
                    fuzzy_rows.append({
                        "so_int_id":         str(so_row.get("INT_ID", "")),
                        "so_import_id":      str(so_row.get("IMPORT_ID", "")),
                        "so_name":           str(so_row.get("SELL_OUT_NAME", "")),
                        "so_brand":          str(so_row.get("SELL_OUT_BRAND", "")),
                        "si_mat_idt":        str(best_match.get("MAT_IDT", "")),
                        "si_mat_lcl_dsc":    str(best_match.get("MAT_LCL_DSC", "")),
                        "si_marca":          str(best_match.get("MARCA", "")),
                        "fuzzy_score":       round(best_score, 1),
                        "match_method":      "FUZZY_NAME_BRAND",
                        "review_status":     "CANDIDATE — DO NOT AUTO-APPROVE",
                        "notes":             "P3 fuzzy match. Requires human review before promotion to sku_mapping.csv",
                    })

    if fuzzy_rows:
        write_csv(UPC_FUZZY_CSV, fuzzy_rows,
                  ["so_int_id", "so_import_id", "so_name", "so_brand",
                   "si_mat_idt", "si_mat_lcl_dsc", "si_marca",
                   "fuzzy_score", "match_method", "review_status", "notes"])
        log.write(f"[{ts()}] Exported {len(fuzzy_rows)} P3 fuzzy UPC candidates → {UPC_FUZZY_CSV}\n")
        log.write(f"  WARNING — Fuzzy candidates must NOT be auto-promoted. Human review required.\n")
    else:
        log.write(f"[{ts()}] No fuzzy candidates generated (skipped or no matches above threshold).\n")

    if unmatched_df is not None:
        write_csv(
            UPC_UNMATCHED_CSV,
            unmatched_df.to_dict("records") if len(unmatched_df) > 0 else [],
            ["INT_ID", "IMPORT_ID", "SELL_OUT_NAME", "SELL_OUT_BRAND", "CBU_ID"],
        )
        log.write(f"[{ts()}] Exported {unmatched_cnt} unmatched SELL_OUT products → {UPC_UNMATCHED_CSV}\n")


# ===========================================================================
# Section A5 — CANAL & CADENA Sentinel / Null Audits
# ===========================================================================

def check_canal_cadena_sentinels(log):
    log.write("\n" + "=" * 70 + "\n")
    log.write("SECTION A5 — CANAL & CADENA SENTINEL / NULL AUDIT\n")
    log.write("=" * 70 + "\n")

    chan_df = load_csv(CATALOG_FILES["channel"])
    cad_df  = load_csv(CATALOG_FILES["cadena"])

    # CANAL sentinel check
    for source in ["MKT_ON", "MKT_OFF"]:
        expected = "ECOMMERCE_MEDIA" if source == "MKT_ON" else "OFFLINE_MEDIA"
        if not chan_df.empty and "source_system" in chan_df.columns:
            rows = chan_df[(chan_df["source_system"] == source) & (chan_df["canal_std"] == expected)]
            if len(rows) > 0:
                log.write(f"[{ts()}] PASS   — {source} has confirmed CANAL sentinel: '{expected}'\n")
            else:
                log.write(f"[{ts()}] WARNING — {source} sentinel '{expected}' not found in channel_mapping.csv\n")
        else:
            log.write(f"[{ts()}] SKIPPED — channel_mapping.csv empty or missing source_system column.\n")

    # CADENA null check for MKT_OFF
    if not cad_df.empty and "source_system" in cad_df.columns:
        mkt_off_rows = cad_df[cad_df["source_system"] == "MKT_OFF"]
        fake_values  = mkt_off_rows[mkt_off_rows["cadena_std"].str.strip().str.len() > 0]
        if len(fake_values) > 0:
            log.write(f"[{ts()}] HARD BLOCKER — MKT_OFF has non-NULL CADENA_STD values in catalog:\n")
            log.write(fake_values[["source_system", "cadena_std", "notes"]].to_string(index=False) + "\n")
            log.write(f"  CADENA_STD for MKT_OFF must be NULL. Explanation belongs in metadata column only.\n")
        else:
            log.write(f"[{ts()}] PASS   — MKT_OFF CADENA_STD correctly set to NULL (no fake values detected).\n")

    # SELL_IN cadena null check
    if not cad_df.empty and "source_system" in cad_df.columns:
        si_rows = cad_df[(cad_df["source_system"] == "SELL_IN") & (cad_df["cadena_std"].str.strip().str.len() > 0)]
        if len(si_rows) > 0:
            log.write(f"[{ts()}] WARNING — SELL_IN has {len(si_rows)} non-NULL CADENA_STD entries.\n")
            log.write(f"  CADENA_STD for SELL_IN must remain NULL until V_D_CLIENT is validated.\n")
        else:
            log.write(f"[{ts()}] PASS   — SELL_IN CADENA_STD correctly remains NULL pending validation.\n")

    # CANAL check: UPC-applicable sources must never have NULL CANAL
    canal_confirmed = set(chan_df["source_system"].unique()) if not chan_df.empty and "source_system" in chan_df.columns else set()
    for source in SOURCE_SYSTEMS:
        if source not in canal_confirmed:
            log.write(f"[{ts()}] WARNING — {source} has no entries in channel_mapping.csv\n")

    # Write unmapped channels and cadenas from catalogs
    unmapped_channels = []
    unmapped_cadenas  = []
    if not chan_df.empty and "mapping_status" in chan_df.columns:
        nr = chan_df[chan_df["mapping_status"] == "NEEDS_REVIEW"]
        unmapped_channels = nr.to_dict("records") if len(nr) > 0 else []
    if not cad_df.empty and "mapping_status" in cad_df.columns:
        nr = cad_df[cad_df["mapping_status"] == "NEEDS_REVIEW"]
        unmapped_cadenas = nr.to_dict("records") if len(nr) > 0 else []

    write_csv(UNMAPPED_CHANNELS_CSV, unmapped_channels,
              ["raw_channel_value", "raw_channel_normalized", "source_system",
               "canal_std", "canal_type", "mapping_status", "is_active", "notes"])
    write_csv(UNMAPPED_CADENAS_CSV, unmapped_cadenas,
              ["raw_cadena_value", "raw_cadena_normalized", "source_system",
               "cadena_std", "cadena_type", "mapping_status", "is_active", "notes"])
    log.write(f"[{ts()}] Exported {len(unmapped_channels)} NEEDS_REVIEW channels → {UNMAPPED_CHANNELS_CSV}\n")
    log.write(f"[{ts()}] Exported {len(unmapped_cadenas)} NEEDS_REVIEW cadenas   → {UNMAPPED_CADENAS_CSV}\n")


# ===========================================================================
# Section A6 — WASTE Physical Column Validation
# ===========================================================================

WASTE_COLUMN_SQL = """
SELECT COLUMN_NAME, DATA_TYPE
FROM PRD_MDP.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'MDP_STG'
  AND TABLE_NAME   = 'FACT_TOPLINE'
  AND (
        UPPER(COLUMN_NAME) LIKE '%WASTE%'
     OR UPPER(COLUMN_NAME) LIKE '%KGS%'
     OR UPPER(COLUMN_NAME) LIKE '%KG%'
     OR UPPER(COLUMN_NAME) LIKE '%VAL%'
     OR UPPER(COLUMN_NAME) LIKE '%AMOUNT%'
     OR UPPER(COLUMN_NAME) LIKE '%VOLUME%'
  )
ORDER BY COLUMN_NAME
"""

EXPECTED_WASTE_COLS = {"WASTE ($)", "WASTE (KG)"}


def check_waste_columns(conn, log):
    log.write("\n" + "=" * 70 + "\n")
    log.write("SECTION A6 — WASTE PHYSICAL COLUMN VALIDATION\n")
    log.write("=" * 70 + "\n")

    log.write(f"[{ts()}] Expected metric columns: {EXPECTED_WASTE_COLS}\n")
    df = run_query(conn, WASTE_COLUMN_SQL, "WASTE_COLUMNS")

    if df is None:
        log.write(f"[{ts()}] SKIPPED — Snowflake unavailable.\n")
        log.write(f"  SQL to run manually:\n{textwrap.indent(WASTE_COLUMN_SQL.strip(), '    ')}\n")
        return

    found_cols = set(df["COLUMN_NAME"].tolist()) if "COLUMN_NAME" in df.columns else set()
    log.write(f"[{ts()}] Columns found matching waste/kg/val/amount pattern:\n")
    log.write(df.to_string(index=False) + "\n")

    for expected in EXPECTED_WASTE_COLS:
        if expected in found_cols:
            log.write(f"  PASS   — Column '{expected}' exists in FACT_TOPLINE.\n")
        else:
            log.write(f"  WARNING — Column '{expected}' NOT found. Verify physical column name before use.\n")


# ===========================================================================
# Section A7 — Nielsen Market Bridge Coverage
# ===========================================================================

NIELSEN_MARKET_QUERIES: dict[str, str] = {
    "EDP_NIELSEN":        "SELECT DISTINCT TRIM(UPPER(MRKT_DSC_SHRT)) AS raw_market_normalized FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IR_YOG_GEL_MT_NLSN_MKT_DIM",
    "PB_NIELSEN":         "SELECT DISTINCT TRIM(UPPER(MRKT_DSC_SHRT)) AS raw_market_normalized FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_SUST_LECHE_ST_NLSN_MKT_DIM",
    "WATER_NIELSEN_RIE":  "SELECT DISTINCT TRIM(UPPER(MRKT_DSC_SHRT)) AS raw_market_normalized FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_RT_NLSN_MKT_DIM",
    "WATER_SCANTRACK":    "SELECT DISTINCT TRIM(UPPER(MRKT_DSC_SHRT)) AS raw_market_normalized FROM PRD_MEX.MEX_DSP_DPH_MKT.VW_IND_AGUA_BNF_ST_NLSN_MKT_DIM",
}


def check_nielsen_market_bridge(conn, log):
    log.write("\n" + "=" * 70 + "\n")
    log.write("SECTION A7 — NIELSEN MARKET BRIDGE COVERAGE\n")
    log.write("=" * 70 + "\n")

    bridge_df = load_csv(CATALOG_FILES["nielsen"])
    confirmed_markets: set[str] = set()
    if not bridge_df.empty and "raw_market_normalized" in bridge_df.columns:
        confirmed_markets = set(
            bridge_df[bridge_df.get("mapping_status", pd.Series(dtype=str)) == "CONFIRMED"][
                "raw_market_normalized"
            ].str.upper()
        )

    new_rows = []

    for source, query in NIELSEN_MARKET_QUERIES.items():
        log.write(f"\n[{ts()}] Profiling market strings for: {source}\n")
        df = run_query(conn, query, source)

        if df is None:
            log.write(f"  SKIPPED — Snowflake unavailable.\n")
            log.write(f"  SQL to run manually:\n    {query}\n")
            continue

        total = len(df)
        mapped = 0
        for _, row in df.iterrows():
            val = str(row["raw_market_normalized"]).strip().upper()
            if val in confirmed_markets:
                mapped += 1
            else:
                new_rows.append({
                    "raw_market_value":      val,
                    "raw_market_normalized": val,
                    "source_system":         source,
                    "canal_std":             "",
                    "cadena_std":            "",
                    "region_std":            "",
                    "market_type":           "",
                    "reading_type":          "",
                    "mapping_status":        "NEEDS_REVIEW",
                    "notes":                 f"Auto-discovered from {source} — requires manual mapping",
                })

        pct = round(mapped / total * 100, 1) if total > 0 else 0.0
        log.write(f"  Total unique MRKT_DSC_SHRT: {total}\n")
        log.write(f"  Mapped in bridge CSV:        {mapped} ({pct}%)\n")
        log.write(f"  Unmapped (NEEDS_REVIEW):     {total - mapped}\n")
        if pct < 100.0:
            log.write(f"  WARNING — {total - mapped} market strings are unmapped. Append to nielsen_market_mapping.csv.\n")

    if new_rows:
        # Append new rows to nielsen_market_mapping.csv
        existing = load_csv(CATALOG_FILES["nielsen"])
        new_df = pd.DataFrame(new_rows)
        combined = pd.concat([existing, new_df], ignore_index=True).drop_duplicates(
            subset=["raw_market_normalized", "source_system"]
        )
        combined.to_csv(CATALOG_FILES["nielsen"], index=False)
        log.write(f"\n[{ts()}] Appended {len(new_rows)} new NEEDS_REVIEW market strings to nielsen_market_mapping.csv\n")
    else:
        log.write(f"\n[{ts()}] All discovered market strings are already in bridge catalog.\n")


# ===========================================================================
# Main
# ===========================================================================

def main():
    conn = get_sf_connection()
    sf_status = "CONNECTED" if conn is not None else "OFFLINE (queries will be printed for manual execution)"

    with open(LOG_FILE, "w", encoding="utf-8") as log:
        log.write("=" * 70 + "\n")
        log.write("MDM CATALOG VALIDATION — NOTEBOOK A\n")
        log.write(f"Run timestamp : {ts()}\n")
        log.write(f"Snowflake     : {sf_status}\n")
        log.write(f"Base dir      : {BASE_DIR}\n")
        log.write("=" * 70 + "\n")

        # Hard-blocker preamble
        log.write("\nHARD BLOCKER RULES (pipeline must stop if any are triggered):\n")
        log.write("  1. CAT_MARCA duplicate on source_system + raw_name_normalized\n")
        log.write("  2. SELL_IN UPC_STD / SKU_EAN_COD is NULL where product grain requires it\n")
        log.write("  3. Fuzzy UPC matches auto-promoted to production catalog\n")
        log.write("  4. MKT_ON or MKT_OFF joined using UPC predicate\n")
        log.write("  5. MKT_OFF joined using CADENA as required predicate\n")
        log.write("  6. Product IDs cast to numeric (leading zeros lost)\n")
        log.write("  7. Required source table or column does not exist\n\n")

        brand_df = check_marca_catalog(log)
        check_brand_coverage(conn, brand_df, log)
        check_upc_golden_source(conn, log)
        check_upc_bridge(conn, log)
        check_canal_cadena_sentinels(log)
        check_waste_columns(conn, log)
        check_nielsen_market_bridge(conn, log)

        log.write("\n" + "=" * 70 + "\n")
        log.write(f"NOTEBOOK A COMPLETE — {ts()}\n")
        log.write(f"Review {LOGS_DIR} for all output files.\n")
        log.write("=" * 70 + "\n")

    print(f"Validation complete. Master log: {LOG_FILE}")


if __name__ == "__main__":
    main()
