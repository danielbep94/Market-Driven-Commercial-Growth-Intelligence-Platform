"""
================================================================================
SCRIPT   : m1_nielsen_classify.py
PROJECT  : MDM Master Catalog Standardization
PHASE    : Phase 3 — Silver Standardization Layer
MAPPING  : M1 — Nielsen Market Strings → canal_std + region_std
================================================================================

PURPOSE
-------
Reads `logs/signoff_03_nielsen_markets.csv` — the 362 unique Nielsen market
description strings collected by Phase 2 sign-off notebook (cell SIGN-OFF #3).

Applies rule-based classification to produce:
  canal_std    → granular Nielsen channel label (wired directly into silver_nielsen.py)
  region_std   → geographic label (city / area / VDM / national)

Also produces intermediate audit columns:
  AGG_LEVEL    → TOTAL | DETAIL
  CANAL_LVL1   → MODERNO | TRADICIONAL | TOTAL_MERCADO
  CANAL_LVL2   → 14 granular sub-channel types (= canal_std)
  REGION_TYPE  → NATIONAL | AREA | VDM | CITY
  REGION_DETAIL→ sub-area refinements (VDM zones, Area 2.1/2.2, Area 6.1/6.2)

Writes results back into the same CSV (in-place overwrite).
Sets REVIEW_STATUS = CONFIRMED for all classified rows.

HOW TO RUN (locally, no Databricks required)
--------------------------------------------
  cd "Market Growth Intelligence"
  pip install pandas numpy          # only if not already installed
  python3 scripts/m1_nielsen_classify.py

  Then commit the updated CSV:
  git add logs/signoff_03_nielsen_markets.csv
  git commit -m "mapping(M1): update Nielsen market classifications"
  git push origin MDM

WHEN TO RE-RUN
--------------
  - New Nielsen CBUs are added to the pipeline (new MRKT_DSC_SHRT values appear)
  - Classification rules are refined (update apply_rules() below)
  - signoff_03_nielsen_markets.csv is refreshed from a new Phase 2 run

INPUT  : logs/signoff_03_nielsen_markets.csv
         Columns expected: MRKT_DSC_SHRT, NIELSEN_SOURCE_TABLE, REVIEW_STATUS

OUTPUT : logs/signoff_03_nielsen_markets.csv (in-place)
         New columns: AGG_LEVEL, CANAL_LVL1, CANAL_LVL2, REGION_TYPE,
                      REGION_STD, REGION_DETAIL, canal_std, region_std
         Updated col: REVIEW_STATUS → CONFIRMED (or NEEDS_REVIEW if SIN_CLASIFICAR)

DOWNSTREAM CONSUMERS
--------------------
  notebooks/phase3_silver/silver_nielsen.py
    Step B: load_mapping_csv("logs/signoff_03_nielsen_markets.csv", key_col="mrkt_dsc_shrt")
            Reads canal_std and region_std columns from this file.
  notebooks/phase3_silver/phase3_mdm_validation.py
    Asserts coverage >= 50% for canal_std and region_std in nielsen_std.

CLASSIFICATION LOGIC
--------------------
  The rules were provided by Victor Hernandez (Danone MX, 2026-06-27).
  Do NOT change rule logic without updating the CHANGELOG below.

CANAL_LVL2 VALUES (14 types)
-----------------------------
  AUTOSERVICIO                        Modern supermarket / self-service
  AUTOSERVICIO_SCANNING               Scanning panel subset of supermarkets
  AUTOSERVICIO_SURTIDO_EXTENDIDO      Extended-assortment supermarkets
  AUTOSERVICIO_SURTIDO_COMPLETO       Full-assortment supermarkets
  AUTOSERVICIO_SURTIDO_ESENCIAL       Essential-assortment supermarkets
  GRANDES_CADENAS_AUTOSERVICIO        Top national supermarket chains
  AUTOSERVICIOS_MAYORISTAS            Wholesale-oriented supermarkets
  PROXIMIDAD                          Proximity / express format stores
  TDC_FARMACIAS_HARD_DISCOUNTERS      Tiendas de Conveniencia + Pharmacies + Discounters
  MODERNO_TOTAL                       All modern trade aggregate
  TRADICIONAL                         Traditional trade (OXXO-style, small stores)
  TRADICIONAL_GRANDES_MINISUPERS      Large traditional + minisupers
  TRADICIONAL_PEQUENAS_ESTANQUILLOS   Small traditional + estanquillos
  TOTAL_MERCADO                       Total market aggregate (all channels)
  SIN_CLASIFICAR                      Could not be classified — manual review needed

CHANGELOG
---------
  2026-06-27  v1.0  Initial version. Rules provided by Victor Hernandez.
                    362 markets classified, 0 SIN_CLASIFICAR.
                    Results: AUTOSERVICIO 124, AUTOSERVICIO_SCANNING 69,
                             TRADICIONAL 62, MODERNO_TOTAL 22, TOTAL_MERCADO 22,
                             AUTOSERVICIO_SURTIDO_EXTENDIDO 14,
                             AUTOSERVICIO_SURTIDO_COMPLETO 14,
                             AUTOSERVICIO_SURTIDO_ESENCIAL 7,
                             TDC_FARMACIAS_HARD_DISCOUNTERS 7,
                             GRANDES_CADENAS_AUTOSERVICIO 5, PROXIMIDAD 5,
                             TRADICIONAL_GRANDES_MINISUPERS 4,
                             AUTOSERVICIOS_MAYORISTAS 4,
                             TRADICIONAL_PEQUENAS_ESTANQUILLOS 3
================================================================================
"""

import pandas as pd
import numpy as np
import re
import os
import datetime

SCRIPT_VERSION = "1.0"
SCRIPT_DATE    = "2026-06-27"
AUTHOR         = "Victor Hernandez / MDM Project"

# ── Cardinality thresholds ────────────────────────────────────────────────────
# Used for validation summary at end of script
CONFIRMED_STATUS   = "CONFIRMED"
UNCLASSIFIED_LABEL = "SIN_CLASIFICAR"

# ── Paths ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT  = os.path.normpath(os.path.join(SCRIPT_DIR, ".."))
INPUT_CSV  = os.path.join(REPO_ROOT, "logs", "signoff_03_nielsen_markets.csv")
OUTPUT_CSV = INPUT_CSV  # in-place overwrite — source of truth for silver_nielsen.py

print(f"m1_nielsen_classify.py  v{SCRIPT_VERSION}  ({SCRIPT_DATE})")
print(f"Repo root : {REPO_ROOT}")
print(f"Input CSV : {INPUT_CSV}")
print(f"Run at    : {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

# ── Load ──────────────────────────────────────────────────────────────────────
df = pd.read_csv(INPUT_CSV)
print(f"Loaded {len(df):,} rows.  Columns: {list(df.columns)}")


# =============================================================================
# STEP 1 — Normalise raw market string
# =============================================================================
def clean_label(text: str) -> str:
    """
    Normalise MRKT_DSC_SHRT for rule matching.

    Transforms applied (in order):
      1. Strip '_MT' suffix (market type tag added by Nielsen)
      2. Replace '_' with ' '
      3. Remove lone dots that are NOT decimal separators
      4. Collapse multiple whitespace to single space
      5. Strip leading/trailing whitespace
      6. Upper-case
    """
    if pd.isna(text):
        return text
    t = str(text).replace('_MT', '').replace('_', ' ')
    t = re.sub(r'(?<!\d)\.(?!\d)', '', t)   # remove dots unless between digits
    t = re.sub(r'\s+', ' ', t).strip().upper()
    return t


df['MRKT_NORM'] = df['MRKT_DSC_SHRT'].apply(clean_label)


# =============================================================================
# STEP 2 — Classification rules
# Author: Victor Hernandez, 2026-06-27
# Edit CHANGELOG above when modifying any rule.
# =============================================================================
def apply_rules(row: pd.Series) -> pd.Series:
    """
    Apply all classification rules to one row.

    Returns a Series with columns:
      [AGG_LEVEL, CANAL_LVL1, CANAL_LVL2, REGION_TYPE, REGION_STD, REGION_DETAIL]
    """
    norm = row['MRKT_NORM']

    # ── Rule 1: Aggregation level ─────────────────────────────────────────────
    # TOTAL = market aggregates (national, multi-area, multi-city)
    # DETAIL = individual city or area markets
    if re.search(
        r'(^| )TOTAL( |$)|(^| )T PAIS( |$)|MX TOTAL'
        r'|AREA [0-9IVX]+ *[+\-]|AREAS [0-9IVX]+',
        norm
    ):
        agg_level = 'TOTAL'
    else:
        agg_level = 'DETAIL'

    # ── Rule 2: Canal Level 1 (broad channel) ─────────────────────────────────
    if re.search(r'TRADICIONAL|TRADICIONALES|TRADICONAL', norm):
        canal_1 = 'TRADICIONAL'
    elif re.search(
        r'MODERNO|AUTOS|AUTOSERVICIOS|AUTOSERVICIO|GRANDES CADENAS'
        r'|PROXIMIDAD|TDC|FARMACIAS|HARD DISCOUNTERS|SCANNING|MAYORISTAS',
        norm
    ):
        canal_1 = 'MODERNO'
    else:
        canal_1 = 'TOTAL_MERCADO' if agg_level == 'TOTAL' else UNCLASSIFIED_LABEL

    # ── Rule 3: Canal Level 2 (granular — becomes canal_std) ─────────────────
    # Rules ordered from MOST SPECIFIC to LEAST SPECIFIC.
    # Do NOT reorder without testing — earlier matches win.
    if re.search(
        r'TRADICIONALES GRANDES|MEXICO GRANDES|MEXICO GDE|GRANDES Y MINISUPERS', norm
    ):
        canal_2 = 'TRADICIONAL_GRANDES_MINISUPERS'
    elif re.search(
        r'TRADICIONALES PEQUENAS|MEXICO PEQUENAS|PEQUENAS Y ESTANQUILLOS', norm
    ):
        canal_2 = 'TRADICIONAL_PEQUENAS_ESTANQUILLOS'
    elif re.search(r'TRADICIONAL|TRADICIONALES|TRADICONAL', norm):
        canal_2 = 'TRADICIONAL'
    elif re.search(r'TDC|FARMACIAS|HARD DISCOUNTERS', norm):
        canal_2 = 'TDC_FARMACIAS_HARD_DISCOUNTERS'
    elif re.search(r'PROXIMIDAD', norm):
        canal_2 = 'PROXIMIDAD'
    elif re.search(r'MAYORISTAS', norm):
        canal_2 = 'AUTOSERVICIOS_MAYORISTAS'
    elif re.search(r'GRANDES CADENAS', norm):
        canal_2 = 'GRANDES_CADENAS_AUTOSERVICIO'
    elif re.search(r'SURTIDO EXTENDIDO|SURT EXTENDIDO', norm):
        canal_2 = 'AUTOSERVICIO_SURTIDO_EXTENDIDO'
    elif re.search(r'SURTIDO COMPLETO|SURT COMPLETO', norm):
        canal_2 = 'AUTOSERVICIO_SURTIDO_COMPLETO'
    elif re.search(r'SURTIDO ESENCIAL|SURT ESENCIAL', norm):
        canal_2 = 'AUTOSERVICIO_SURTIDO_ESENCIAL'
    elif re.search(r'SCANNING|SCANTRACK', norm):
        canal_2 = 'AUTOSERVICIO_SCANNING'
    elif re.search(r'AUTOS|AUTOSERVICIOS|AUTOSERVICIO', norm):
        canal_2 = 'AUTOSERVICIO'
    elif re.search(r'MODERNO', norm):
        canal_2 = 'MODERNO_TOTAL'
    else:
        canal_2 = 'TOTAL_MERCADO' if agg_level == 'TOTAL' else UNCLASSIFIED_LABEL

    # ── Rule 4: Region type ────────────────────────────────────────────────────
    if re.search(r'TOTAL MEXICO|MX TOTAL|T PAIS|TOTAL \+MEXICO', norm):
        reg_type = 'NATIONAL'
    elif re.search(
        r'AREA [0-9]+\.[0-9]+|AREA [0-9]+|AREA [IVX]+|AREAS [0-9IVX]+', norm
    ):
        reg_type = 'AREA'
    elif re.search(r'VDM|VALLE DE MEXICO', norm):
        reg_type = 'VDM'
    else:
        reg_type = 'CITY'

    # ── Rule 5: Region std (geography label) ───────────────────────────────────
    if reg_type == 'NATIONAL':
        reg_std = 'TOTAL_MEXICO'
    elif re.search(r'AREA 1 *\+ *AREA 2|AREAS 1 *\+ *2', norm):
        reg_std = 'AREA_1_2'
    elif re.search(r'AREA 3 *[+\-] *AREA 6|AREAS 3 *\+ *4 *\+ *5 *\+ *6', norm):
        reg_std = 'AREA_3_6'
    elif re.search(r'AREA 1|AREA I( |$)', norm):
        reg_std = 'AREA_1'
    elif re.search(r'AREA 2|AREA II( |$)|AREA 2\.1|AREA 2\.2', norm):
        reg_std = 'AREA_2'
    elif re.search(r'AREA 3|AREA III( |$)', norm):
        reg_std = 'AREA_3'
    elif re.search(r'AREA 4|AREA IV( |$)', norm):
        reg_std = 'AREA_4'
    elif re.search(r'AREA 5|AREA V( |$)', norm):
        reg_std = 'AREA_5'
    elif re.search(r'AREA 6|AREA VI( |$)|AREA 6\.1|AREA 6\.2', norm):
        reg_std = 'AREA_6'
    elif reg_type == 'VDM':
        reg_std = 'VDM'
    else:
        # For CITY-type markets: strip the canal-type prefix so only
        # the city/geography name remains as region_std.
        # Prefixes ordered LONGEST → SHORTEST to avoid partial matches.
        prefixes = (
            r'^(TOTAL AUTOS SCANNING|TOTAL AUTOS|AUTOSERVICIOS SCANNING'
            r'|GRANDES CADENAS DE AUTOSERVICIO'
            r'|TDC \+ FARMACIAS CAD \+ HARD DISCOUNTERS'
            r'|AUTOSERVICIOS MAYORISTAS|AUTOSERVICIOS'
            r'|CANAL TRADICIONAL|CANAL MODERNO'
            r'|TRADICIONALES GRANDES|TRADICIONALES PEQUENAS'
            r'|TRADICIONALES|TRADICIONAL|TRADICONAL'
            r'|AUTOS SCANNING|AUTOS'
            r'|MODERNO|C MODERNO|C PROXIMIDAD|PROXIMIDAD|TOTAL) +'
        )
        reg_std = re.sub(prefixes, '', norm).strip()

    # ── Rule 6: Region detail (sub-area refinements) ────────────────────────────
    if   re.search(r'AREA 2\.1', norm):          reg_det = 'AREA_2_1_CONTINENTAL'
    elif re.search(r'AREA 2\.2', norm):          reg_det = 'AREA_2_2_GOLFO'
    elif re.search(r'AREA 6\.1', norm):          reg_det = 'AREA_6_1_PENINSULA'
    elif re.search(r'AREA 6\.2', norm):          reg_det = 'AREA_6_2_SUR'
    elif re.search(r'VDM ZONA I|VDM ZONA 1',    norm): reg_det = 'VDM_ZONA_1'
    elif re.search(r'VDM ZONA II|VDM ZONA 2',   norm): reg_det = 'VDM_ZONA_2'
    elif re.search(r'VDM ZONA III|VDM ZONA 3',  norm): reg_det = 'VDM_ZONA_3'
    elif re.search(r'VDM ZONA IV|VDM ZONA 4',   norm): reg_det = 'VDM_ZONA_4'
    else:                                               reg_det = np.nan

    return pd.Series([agg_level, canal_1, canal_2, reg_type, reg_std, reg_det])


# =============================================================================
# STEP 3 — Apply rules and wire output columns
# =============================================================================
df[['AGG_LEVEL', 'CANAL_LVL1', 'CANAL_LVL2', 'REGION_TYPE', 'REGION_STD', 'REGION_DETAIL']] = \
    df.apply(apply_rules, axis=1)

# canal_std = CANAL_LVL2 — the column consumed by silver_nielsen.py
df['canal_std']  = df['CANAL_LVL2']

# region_std = REGION_STD — geography label consumed by silver_nielsen.py
df['region_std'] = df['REGION_STD']

# REVIEW_STATUS: CONFIRMED if classified, NEEDS_REVIEW if SIN_CLASIFICAR
unclassified = df['canal_std'] == UNCLASSIFIED_LABEL
df.loc[~unclassified, 'REVIEW_STATUS'] = CONFIRMED_STATUS
df.loc[unclassified,  'REVIEW_STATUS'] = 'NEEDS_REVIEW'

# Drop the working normalisation column — keep output clean
df.drop(columns=['MRKT_NORM'], inplace=True)

# =============================================================================
# STEP 4 — Save
# =============================================================================
df.to_csv(OUTPUT_CSV, index=False)
print(f"✅ Written {len(df):,} rows → {OUTPUT_CSV}")

# =============================================================================
# STEP 5 — Validation summary
# =============================================================================
n_total        = len(df)
n_confirmed    = (df['REVIEW_STATUS'] == CONFIRMED_STATUS).sum()
n_unclassified = unclassified.sum()

print()
print("── canal_std distribution ──────────────────────────────────────────────")
print(df['canal_std'].value_counts().to_string())

print()
print("── REVIEW_STATUS ───────────────────────────────────────────────────────")
print(df['REVIEW_STATUS'].value_counts().to_string())

print()
print("── region_std sample (first 15 rows) ───────────────────────────────────")
print(df[['MRKT_DSC_SHRT', 'canal_std', 'region_std']].head(15).to_string(index=False))

print()
print(f"── Summary ─────────────────────────────────────────────────────────────")
print(f"  Total rows     : {n_total:,}")
print(f"  CONFIRMED      : {n_confirmed:,}  ({n_confirmed/n_total*100:.1f}%)")
print(f"  NEEDS_REVIEW   : {n_unclassified:,}  ({n_unclassified/n_total*100:.1f}%)")

if n_unclassified > 0:
    print()
    print(f"⚠️  {n_unclassified} rows are SIN_CLASIFICAR — add rules for these patterns:")
    print(df[unclassified][['MRKT_DSC_SHRT', 'CANAL_LVL2', 'REGION_STD']].to_string(index=False))
    print()
    print("To fix: add new elif clauses in apply_rules() → Rule 3 (CANAL_LVL2)")
    print("Then update CHANGELOG at the top of this file.")
else:
    print()
    print("✅ All rows classified — zero SIN_CLASIFICAR.")
    print("   Ready to commit and re-run phase3_mdm_validation.")
