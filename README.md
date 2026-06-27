# Market Driven Commercial Growth Intelligence Platform

**Organization:** Danone Mexico  
**Owner:** Victor Hernandez  
**Branch:** `MDM`  
**Status:** Phase 3 — Silver Standardization Layer ✅ COMPLETE (Pending Final Validation)

---

## Project Objective

Build a unified **MDM Master Catalog Standardization** pipeline that bridges five commercial data sources into a common analytical vocabulary:

| Dimension | Standard column | Source |
|---|---|---|
| Brand | `marca_std` | `LV2_UMB_BRD_DSC` (SELL_IN) / `BRAND` (SELL_OUT) |
| Retail chain | `cadena_std` | M3 chain mapping (SELL_OUT) / NULL (SELL_IN, MKT_OFF) |
| Trade channel | `canal_std` | M4 format mapping / M1 Nielsen mapping / MEDIO |
| Geography | `region_std` | M1 Nielsen market mapping |

---

## Phase Status

| Phase | Description | Status |
|---|---|---|
| Phase 1 | Assessment & catalog extraction | ✅ Complete |
| Phase 2 | MDM Sign-Off & data quality | ✅ Complete |
| Phase 3 | Silver standardization — all 5 notebooks | ✅ Ran successfully |
| Phase 3 | M1–M4 manual mappings | ✅ All CONFIRMED |
| Phase 3 | phase3_mdm_validation | 🔲 Ready to run |
| Phase 4 | KPI features | 🔲 Not started |
| Phase 5 | ML models | 🔲 Not started |

---

## Folder Structure

```
Market Growth Intelligence/
├── configs/                          # All YAML configuration files (Rule R12)
│   ├── brand_crosswalk.yaml
│   ├── column_types_snapshot.yaml
│   ├── dq_thresholds.yaml
│   ├── phase3_architectural_decisions.yaml  # Structural NULL decisions (R9)
│   ├── pipeline_config.yaml
│   ├── tech_debt.yaml
│   └── snowflake_creds.py            # SECRET — git-ignored, must exist locally
│
├── docs/                             # Architecture, contracts, governance
├── homologation/                     # SKU mapping tables
├── logs/                             # All notebook outputs + mapping CSVs
│   ├── sell_in_std.csv               # 8.4 MB
│   ├── sell_out_std.csv              # 24 MB
│   ├── nielsen_std.csv               # 34 KB (362 market strings)
│   ├── nielsen_facts_std.csv         # 17 MB
│   ├── mkt_on_std.csv                # 997 KB
│   ├── mkt_off_std.csv               # 12 MB
│   ├── signoff_03_nielsen_markets.csv          # M1: 362/362 CONFIRMED
│   ├── signoff_05_store_chain_classification.csv  # M3: 19/19 CONFIRMED
│   └── signoff_05_store_format_classification.csv # M4: 86/86 CONFIRMED
│
├── notebooks/
│   ├── phase2_mdm_signoff.py         # Phase 2 (completed — do not modify)
│   ├── phase3_mdm_validation.py      # ← RUN THIS NEXT
│   └── phase3_silver/
│       ├── silver_homologation_apply.py   # Shared utilities (%run'd by all)
│       ├── silver_sell_in.py
│       ├── silver_sell_out.py
│       ├── silver_nielsen.py
│       ├── silver_mkt_on_std.py
│       └── silver_mkt_off_std.py
│
├── scripts/
│   ├── README.md
│   └── m1_nielsen_classify.py        # M1 Nielsen classification
│
├── SEMANTIC_LAYOUTS/                 # Nielsen semantic join definitions
├── SDD.md                            # Software Design Document
└── README.md                         # This file
```

---

## How to Run

### Order of Execution (Phase 3 silver already complete)

```
Next step → notebooks/phase3_mdm_validation.py
```

Each silver notebook `%run`s `silver_homologation_apply.py` automatically.

### Required Files Before Running Validation

| File | Status |
|---|---|
| `configs/snowflake_creds.py` | Must exist (git-ignored) |
| `configs/brand_crosswalk.yaml` | ✅ Present |
| `logs/signoff_03_nielsen_markets.csv` | ✅ 362/362 CONFIRMED |
| `logs/signoff_05_store_chain_classification.csv` | ✅ 19/19 CONFIRMED |
| `logs/signoff_05_store_format_classification.csv` | ✅ 86/86 CONFIRMED |
| `logs/sell_in_std.csv` | ✅ Present |
| `logs/sell_out_std.csv` | ✅ Present |
| `logs/nielsen_std.csv` | ✅ Present |
| `logs/mkt_on_std.csv` | ✅ Present |
| `logs/mkt_off_std.csv` | ✅ Present |

---

## Expected Outputs

| Dataset | Rows | Size |
|---|---|---|
| `sell_in_std` | 49,815 | 8.4 MB |
| `sell_out_std` | ~100K+ | 24 MB |
| `nielsen_std` | 362 | 34 KB |
| `nielsen_facts_std` | 1,092,556 | 17 MB |
| `mkt_on_std` | 7,282 | 997 KB |
| `mkt_off_std` | ~100K+ | 12 MB |

---

## Validation Assertions (phase3_mdm_validation.py)

| ID | Rule | Check |
|---|---|---|
| A1 | R6 | MKT_ON has no UPC join column |
| A2 | R7 | MKT_OFF has no UPC join column |
| A3 | R8 | MKT_OFF has no CADENA join column |
| A4 | R9 | MKT_OFF `cadena_std` = NULL for 100% rows |
| A5 | R10 | SELL_OUT bridge columns present |
| A6 | R10 | Zero EAN→MAT_IDT fanout in sell_out_std |
| A7 | R11 | No fuzzy matches in any *_std output |
| A8 | R15 | Row count reconciliation |
| A9 | — | NULL key dimensions accounted for in quarantine |

---

## Known Non-Blocking Warnings

| Warning | Root Cause | Reference |
|---|---|---|
| `sell_in.cadena_std = 0%` | SELL_IN ships to CEDIS — no chain granularity (M2) | `phase3_architectural_decisions.yaml` |
| `mkt_off.cadena_std = 100% NULL` | Offline media has no chain dimension (R9) | Architectural decision |
| `mkt_on.cadena_std = 86.89% NULL` | Digital campaigns are brand-level (R9) | Expected |
| `A8: nielsen_std = 362 vs 2940` | Market dim = 362 unique strings; facts = 1.09M rows | Fix expected count |

---

## Locked Architectural Rules

| Rule | Description |
|---|---|
| MAT_IDT | Unique SAP product key — grain of V_D_ITEM |
| SKU_EAN_COD | Barcode attribute — NOT a unique key |
| R4 | Active catalog: `SKU_EAN_COD IS NOT NULL` — never `MAT_ACT_FLG = 1` |
| R9 | Structural NULLs in `cadena_std` are correct for SELL_IN and MKT_OFF |
| R10 | SELL_OUT EAN: `MIN(MAT_IDT) GROUP BY EAN` — 1:1 guarantee |
| R11 | Fuzzy matches: quarantine-only, never auto-promoted |
| R12 | All mapping rules from YAML/CSV — never hardcoded |
| R13 | `nielsen_std` unique on `MRKT_DSC_SHRT` before any fact join |
| R14 | `load_mapping_csv()` asserts uniqueness before every join |
| R15 | `assert_row_count_exact()` after every left join |

---

## Tech Debt

See `configs/tech_debt.yaml` for known items. Key item:

- **TD-001** (MEDIUM): Nielsen period CTEs hardcoded through 2026-12 — will break Jan 2027.
  Files: `SEMANTIC_LAYOUTS/EDP_NIELSEN/`, `SEMANTIC_LAYOUTS/WATER_NEILSEN/`
