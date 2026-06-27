# scripts/

Local Python helper scripts for the **MDM Master Catalog Standardization** project.

These scripts run **locally** (no Databricks, no Snowflake connection required).  
They manipulate mapping CSVs that feed the Phase 3 silver notebooks.

---

## Scripts

### `m1_nielsen_classify.py` — M1 Nielsen Market Mapping

| Field | Value |
|---|---|
| **Input** | `logs/signoff_03_nielsen_markets.csv` |
| **Output** | Same file (in-place) with new columns added |
| **Downstream** | `notebooks/phase3_silver/silver_nielsen.py` → Step B |
| **Version** | v1.0 — 2026-06-27 |
| **Author** | Victor Hernandez |

**What it does:**  
Classifies each of the 362 Nielsen market description strings (`MRKT_DSC_SHRT`)
into a standard channel (`canal_std`) and geography (`region_std`) using
regex-based rules supplied by the business team.

**New columns added:**

| Column | Description | Values |
|---|---|---|
| `canal_std` | Granular Nielsen channel = `CANAL_LVL2` | 14 types — see script header |
| `region_std` | Geography label (city, area, VDM, national) | Free-text |
| `AGG_LEVEL` | Market aggregation level | `TOTAL` / `DETAIL` |
| `CANAL_LVL1` | Broad channel | `MODERNO` / `TRADICIONAL` / `TOTAL_MERCADO` |
| `CANAL_LVL2` | Granular channel (= `canal_std`) | 14 types |
| `REGION_TYPE` | Geographic scope | `NATIONAL` / `AREA` / `VDM` / `CITY` |
| `REGION_DETAIL` | Sub-area (VDM zones, Area 2.1/6.1 etc.) | Optional |

**How to run:**
```bash
cd "Market Growth Intelligence"
python3 scripts/m1_nielsen_classify.py
```

**How to update rules:**  
Edit the `apply_rules()` function inside the script.  
Always update the `CHANGELOG` section at the top of the file.

**Results (v1.0 — 2026-06-27):**
```
AUTOSERVICIO                      124
AUTOSERVICIO_SCANNING              69
TRADICIONAL                        62
MODERNO_TOTAL                      22
TOTAL_MERCADO                      22
AUTOSERVICIO_SURTIDO_EXTENDIDO     14
AUTOSERVICIO_SURTIDO_COMPLETO      14
AUTOSERVICIO_SURTIDO_ESENCIAL       7
TDC_FARMACIAS_HARD_DISCOUNTERS      7
GRANDES_CADENAS_AUTOSERVICIO        5
PROXIMIDAD                          5
TRADICIONAL_GRANDES_MINISUPERS      4
AUTOSERVICIOS_MAYORISTAS            4
TRADICIONAL_PEQUENAS_ESTANQUILLOS   3
─────────────────────────────────────
CONFIRMED: 362 / 362  (100%)
SIN_CLASIFICAR: 0
```

---

## Mapping files managed by these scripts

| Mapping | CSV file | Script | Status |
|---|---|---|---|
| M1 — Nielsen markets | `logs/signoff_03_nielsen_markets.csv` | `m1_nielsen_classify.py` | ✅ CONFIRMED 362/362 |
| M2 — SELL_IN cadena | TBD (pending discovery) | Databricks notebook | 🔲 Discovery pending |
| M3 — SELL_OUT chain → cadena_std | `logs/signoff_05_store_chain_classification.csv` | Manual fill | ✅ CONFIRMED 19/19 |
| M4 — SELL_OUT format → canal_std | `logs/signoff_05_store_format_classification.csv` | Manual fill | ✅ CONFIRMED 86/86 |

---

## Adding a new script

1. Name it `<mapping_id>_<description>.py` (e.g. `m2_sell_in_cadena.py`)
2. Add a module-level docstring with: PURPOSE, INPUT, OUTPUT, HOW TO RUN, CHANGELOG
3. Register it in this README under **Scripts**
4. Commit with message: `feat(scripts): add <name>`
