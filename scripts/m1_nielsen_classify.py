"""
M1 Nielsen Market Classification Script
Runs locally against logs/signoff_03_nielsen_markets.csv
Applies the user's classification rules and writes canal_std + region_std
back into the CSV ready for commit.
"""
import pandas as pd
import numpy as np
import re
import os

# ── paths ────────────────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT  = os.path.normpath(os.path.join(SCRIPT_DIR, ".."))
INPUT_CSV  = os.path.join(REPO_ROOT, "logs", "signoff_03_nielsen_markets.csv")
OUTPUT_CSV = INPUT_CSV   # overwrite in-place

# ── load ─────────────────────────────────────────────────────────────────────
df = pd.read_csv(INPUT_CSV)
print(f"Loaded {len(df)} rows from {INPUT_CSV}")
print(f"Columns: {list(df.columns)}")

# ── normalise market string ───────────────────────────────────────────────────
def clean_label(text):
    if pd.isna(text):
        return text
    t = str(text).replace('_MT', '').replace('_', ' ')
    t = re.sub(r'(?<!\d)\.(?!\d)', '', t)
    t = re.sub(r'\s+', ' ', t).strip().upper()
    return t

df['MRKT_NORM'] = df['MRKT_DSC_SHRT'].apply(clean_label)

# ── classification rules (user-supplied) ─────────────────────────────────────
def apply_rules(row):
    norm = row['MRKT_NORM']

    # 1. AGGREGATION LEVEL
    if re.search(
        r'(^| )TOTAL( |$)|(^| )T PAIS( |$)|MX TOTAL|AREA [0-9IVX]+ *[+\-]|AREAS [0-9IVX]+',
        norm):
        agg_level = 'TOTAL'
    else:
        agg_level = 'DETAIL'

    # 2. CANAL LVL 1
    if re.search(r'TRADICIONAL|TRADICIONALES|TRADICONAL', norm):
        canal_1 = 'TRADICIONAL'
    elif re.search(
        r'MODERNO|AUTOS|AUTOSERVICIOS|AUTOSERVICIO|GRANDES CADENAS|'
        r'PROXIMIDAD|TDC|FARMACIAS|HARD DISCOUNTERS|SCANNING|MAYORISTAS', norm):
        canal_1 = 'MODERNO'
    else:
        canal_1 = 'TOTAL_MERCADO' if agg_level == 'TOTAL' else 'SIN_CLASIFICAR'

    # 3. CANAL LVL 2
    if re.search(r'TRADICIONALES GRANDES|MEXICO GRANDES|MEXICO GDE|GRANDES Y MINISUPERS', norm):
        canal_2 = 'TRADICIONAL_GRANDES_MINISUPERS'
    elif re.search(r'TRADICIONALES PEQUENAS|MEXICO PEQUENAS|PEQUENAS Y ESTANQUILLOS', norm):
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
        canal_2 = 'TOTAL_MERCADO' if agg_level == 'TOTAL' else 'SIN_CLASIFICAR'

    # 4. REGION TYPE
    if re.search(r'TOTAL MEXICO|MX TOTAL|T PAIS|TOTAL \+MEXICO', norm):
        reg_type = 'NATIONAL'
    elif re.search(r'AREA [0-9]+\.[0-9]+|AREA [0-9]+|AREA [IVX]+|AREAS [0-9IVX]+', norm):
        reg_type = 'AREA'
    elif re.search(r'VDM|VALLE DE MEXICO', norm):
        reg_type = 'VDM'
    else:
        reg_type = 'CITY'

    # 5. REGION STD
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
        # strip long prefixes first so only the city/geography remains
        prefixes = (
            r'^(TOTAL AUTOS SCANNING|TOTAL AUTOS|AUTOSERVICIOS SCANNING|'
            r'GRANDES CADENAS DE AUTOSERVICIO|TDC \+ FARMACIAS CAD \+ HARD DISCOUNTERS|'
            r'AUTOSERVICIOS MAYORISTAS|AUTOSERVICIOS|CANAL TRADICIONAL|CANAL MODERNO|'
            r'TRADICIONALES GRANDES|TRADICIONALES PEQUENAS|TRADICIONALES|TRADICIONAL|'
            r'TRADICONAL|AUTOS SCANNING|AUTOS|MODERNO|C MODERNO|C PROXIMIDAD|PROXIMIDAD|TOTAL) +'
        )
        reg_std = re.sub(prefixes, '', norm).strip()

    # 6. REGION DETAIL (sub-area refinements)
    if   re.search(r'AREA 2\.1', norm):       reg_det = 'AREA_2_1_CONTINENTAL'
    elif re.search(r'AREA 2\.2', norm):       reg_det = 'AREA_2_2_GOLFO'
    elif re.search(r'AREA 6\.1', norm):       reg_det = 'AREA_6_1_PENINSULA'
    elif re.search(r'AREA 6\.2', norm):       reg_det = 'AREA_6_2_SUR'
    elif re.search(r'VDM ZONA I|VDM ZONA 1',   norm): reg_det = 'VDM_ZONA_1'
    elif re.search(r'VDM ZONA II|VDM ZONA 2',  norm): reg_det = 'VDM_ZONA_2'
    elif re.search(r'VDM ZONA III|VDM ZONA 3', norm): reg_det = 'VDM_ZONA_3'
    elif re.search(r'VDM ZONA IV|VDM ZONA 4',  norm): reg_det = 'VDM_ZONA_4'
    else:                                              reg_det = np.nan

    return pd.Series([agg_level, canal_1, canal_2, reg_type, reg_std, reg_det])

# ── apply ─────────────────────────────────────────────────────────────────────
df[['AGG_LEVEL', 'CANAL_LVL1', 'CANAL_LVL2', 'REGION_TYPE', 'REGION_STD', 'REGION_DETAIL']] = \
    df.apply(apply_rules, axis=1)

# ── wire canal_std and region_std ─────────────────────────────────────────────
# canal_std = CANAL_LVL2 (granular Nielsen channel classification)
# region_std = REGION_STD (geography, stripped of canal prefix)
df['canal_std']  = df['CANAL_LVL2']
df['region_std'] = df['REGION_STD']

# ── update REVIEW_STATUS → CONFIRMED for all classified rows ──────────────────
unclassified = df['canal_std'].isin(['SIN_CLASIFICAR'])
df.loc[~unclassified, 'REVIEW_STATUS'] = 'CONFIRMED'
df.loc[unclassified,  'REVIEW_STATUS'] = 'NEEDS_REVIEW'

# ── drop helper column ────────────────────────────────────────────────────────
df.drop(columns=['MRKT_NORM'], inplace=True)

# ── save ──────────────────────────────────────────────────────────────────────
df.to_csv(OUTPUT_CSV, index=False)
print(f"\n✅ Written {len(df)} rows to {OUTPUT_CSV}")

# ── summary ───────────────────────────────────────────────────────────────────
print("\n── canal_std distribution ──────────────────────────────────────────")
print(df['canal_std'].value_counts().to_string())
print("\n── REVIEW_STATUS ───────────────────────────────────────────────────")
print(df['REVIEW_STATUS'].value_counts().to_string())
print("\n── region_std sample (first 10) ────────────────────────────────────")
print(df[['MRKT_DSC_SHRT', 'canal_std', 'region_std']].head(10).to_string(index=False))
n_unclassified = unclassified.sum()
if n_unclassified > 0:
    print(f"\n⚠️  {n_unclassified} rows still SIN_CLASIFICAR — review manually:")
    print(df[unclassified][['MRKT_DSC_SHRT', 'CANAL_LVL2', 'REGION_STD']].to_string(index=False))
