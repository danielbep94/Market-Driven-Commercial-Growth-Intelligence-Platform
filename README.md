# Market Driven Commercial Growth Intelligence Platform

**Organization:** Danone Mexico  
**Owner:** Victor Hernandez  
**Branch:** `phase4-gold-kpi`  
**Last Updated:** 2026-06-27  
**Status:** 🟡 Phase 4 Gold KPI — **IN PROGRESS** — commit 187fc77

---

## Project Objective

Build a unified **MDM Master Catalog Standardization** pipeline that bridges five commercial data sources into a common analytical vocabulary across brand, channel, chain, and geography dimensions.

| Dimension | Standard column | Sources |
|---|---|---|
| Brand | `marca_std` | `LV2_UMB_BRD_DSC` (SELL_IN) · `BRAND` (SELL_OUT/MKT_OFF) · `MARCA` (MKT_ON) |
| Retail chain | `cadena_std` | M3 chain mapping (SELL_OUT) · NULL by design (SELL_IN, MKT_OFF) |
| Trade channel | `canal_std` | M4 format mapping (SELL_OUT) · M1 Nielsen market · MEDIO (MKT_OFF) |
| Geography | `region_std` | M1 Nielsen market mapping |

---

## Phase Status

| Phase | Description | Status |
|---|---|---|
| Phase 1 | Assessment & catalog extraction | ✅ Complete |
| Phase 2 | MDM Sign-Off & data quality | ✅ Complete |
| Phase 3 — Silver | All 5 standardization notebooks | ✅ Complete |
| Phase 3 — Mappings | M1–M4 manual mappings | ✅ All CONFIRMED |
| Phase 3 — Validation | `phase3_mdm_validation.py` | ✅ **🟢 GATE: CLEAR** (2026-06-27) |
| Phase 4 — Gold KPI | `notebooks/phase4_gold/` (7 notebooks) | 🟡 In progress — commit 187fc77 |
| Phase 5 | ML models | 🔲 Not started |

---

## Folder Structure

```
Market Growth Intelligence/
├── configs/
│   ├── brand_crosswalk.yaml                          # MARCA variants → marca_std
│   ├── column_types_snapshot.yaml
│   ├── dq_thresholds.yaml                            # Coverage thresholds per dimension
│   ├── phase3_architectural_decisions.yaml           # Structural NULL decisions (R9/M2)
│   ├── pipeline_config.yaml
│   ├── tech_debt.yaml                                # Tracked tech debt items
│   └── snowflake_creds.py                            # SECRET — git-ignored, must exist locally
│
├── docs/
│   ├── phase3_readiness/                             # Pre-validation readiness review
│   │   ├── phase3_project_readiness_report.md
│   │   ├── m2_cadena_recommendation.txt              # M2 cadena discovery outcome
│   │   └── orphan_files_audit.md
│   └── ...
│
├── logs/                                             # All notebook outputs (committed to repo)
│   ├── sell_in_std.csv                               # 49,815 rows
│   ├── sell_out_std.csv                              # 100,000 rows
│   ├── nielsen_std.csv                               # 362 market strings
│   ├── nielsen_facts_std.csv                         # ~1M fact rows
│   ├── mkt_on_std.csv                                # 7,282 rows
│   ├── mkt_off_std.csv                               # 100,000 rows
│   ├── signoff_03_nielsen_markets.csv                # M1: 362/362 CONFIRMED
│   ├── signoff_05_store_chain_classification.csv     # M3: 19/19 CONFIRMED
│   ├── signoff_05_store_format_classification.csv    # M4: 86/86 CONFIRMED
│   ├── phase3_standardization_audit_log.txt          # Primary validation log ← read this
│   ├── phase3_mapping_coverage_report.txt            # Coverage by dimension
│   ├── phase3_null_rate_validation.txt               # NULL rates per column
│   ├── phase3_row_count_reconciliation.txt           # Row count snapshot
│   └── phase3_join_safety_assertions.txt             # A1–A3 JOIN_REGISTRY results
│
├── notebooks/
│   ├── phase2_mdm_signoff.py                         # Phase 2 (completed — do not modify)
│   ├── phase3_mdm_validation.py                      # Master validation ← gate confirmed CLEAR
│   └── phase3_silver/
│       ├── silver_homologation_apply.py              # Shared utilities (%run'd by all silver notebooks)
│       ├── silver_sell_in.py                         # SELL_IN standardization
│       ├── silver_sell_out.py                        # SELL_OUT standardization
│       ├── silver_nielsen.py                         # Nielsen market dim standardization
│       ├── silver_mkt_on_std.py                      # Digital MKT_ON standardization
│       └── silver_mkt_off_std.py                     # Offline MKT_OFF standardization
│
├── scripts/
│   ├── README.md                                     # Scripts documentation
│   └── m1_nielsen_classify.py                        # M1 Nielsen market classification (local)
│
├── SEMANTIC_LAYOUTS/                                 # Nielsen semantic join definitions
├── SDD.md                                            # Software Design Document
└── README.md                                         # This file
```

---

## How to Run (Phase 4 — next)

Phase 3 is complete. All `*_std.csv` outputs are committed to `logs/`. Validation gate is 🟢 CLEAR.

### If re-running silver notebooks (e.g. after mapping updates)

Run in this order — each depends on `silver_homologation_apply.py` via `%run`:

```
1. notebooks/phase3_silver/silver_sell_in.py
2. notebooks/phase3_silver/silver_sell_out.py
3. notebooks/phase3_silver/silver_nielsen.py
4. notebooks/phase3_silver/silver_mkt_on_std.py
5. notebooks/phase3_silver/silver_mkt_off_std.py
6. notebooks/phase3_mdm_validation.py
```

---

## How to Run — Phase 4 (Gold KPI Layer)

> **IMPORTANT:** Phase 4 performs zero Snowflake writes. All outputs land on DBFS and `logs/`.
> B14 + B15 pre-confirmed 2026-06-27: zero Snowflake write or mutation statements in any Phase 4 notebook.

### Execution Order

```
# Step 0 — Pre-step: validate cross-source brand alignment (already implemented)
nnotebooks/validate_cross_source_joins_phase_d.py

# Step 1 — Shared utilities (run first — all other notebooks %run this)
notebooks/phase4_gold/gold_kpi_utils.py   [RUN_MODE = FULL]

# Steps 2–5 — Source Gold KPIs (independent — run in parallel)
notebooks/phase4_gold/gold_sell_in.py       → gold_sell_in_kpi.csv
                                               gold_sell_in_kpi_master.csv
notebooks/phase4_gold/gold_sell_out.py      → gold_sell_out_kpi.csv
notebooks/phase4_gold/gold_investment.py    → gold_investment_kpi.csv
notebooks/phase4_gold/gold_nielsen.py       → gold_nielsen_kpi.csv
                                               gold_nielsen_kpi_master.csv

# Step 6 — Master commercial KPI table (depends on steps 2–5)
notebooks/phase4_gold/gold_commercial_kpi.py → gold_commercial_kpi.csv

# Step 7 — Validation gate
notebooks/phase4_gold_validation.py
# Expected: PHASE 4 GATE: 🟢 CLEAR
```

### Gold Output Locations

| File | Location | Grain |
|---|---|---|
| `gold_sell_in_kpi.csv` | `dbfs:/mnt/mdp/mdm/phase4_gold/data/` | fecha_month × marca_std × canal_std × cbu |
| `gold_sell_in_kpi_master.csv` | `dbfs:/mnt/mdp/mdm/phase4_gold/data/` | fecha_month × marca_std × canal_std |
| `gold_sell_out_kpi.csv` | `dbfs:/mnt/mdp/mdm/phase4_gold/data/` | fecha_month × marca_std × canal_std × cadena_std |
| `gold_investment_kpi.csv` | `dbfs:/mnt/mdp/mdm/phase4_gold/data/` | fecha_month × marca_std × canal_std × brand_owner_type |
| `gold_nielsen_kpi.csv` | `dbfs:/mnt/mdp/mdm/phase4_gold/data/` | fecha_month × canal_std × region_std |
| `gold_nielsen_kpi_master.csv` | `dbfs:/mnt/mdp/mdm/phase4_gold/data/` | fecha_month × canal_std |
| `gold_commercial_kpi.csv` | `dbfs:/mnt/mdp/mdm/phase4_gold/data/` | fecha_month × marca_std × canal_std × cadena_std |

Audit files (repo-safe, small):
- `logs/phase4_standardization_audit_log.txt`
- `logs/phase4_gold_coverage_report.txt`
- `logs/phase4_row_count_reconciliation.txt`
- `logs/phase4_kpi_registry.csv`
```

> ⚠️ Always re-run `phase3_mdm_validation.py` after any silver notebook change.

### Required files

| File | Status |
|---|---|
| `configs/snowflake_creds.py` | Must exist locally (git-ignored) |
| `logs/signoff_03_nielsen_markets.csv` | ✅ 362/362 CONFIRMED |
| `logs/signoff_05_store_chain_classification.csv` | ✅ 19/19 CONFIRMED |
| `logs/signoff_05_store_format_classification.csv` | ✅ 86/86 CONFIRMED |

---

## Validated Outputs (Phase 3 Gate Run — 2026-06-27 17:37 UTC)

| Dataset | Rows | marca_std | canal_std | cadena_std | region_std |
|---|---|---|---|---|---|
| `sell_in_std` | 49,815 | 97.2% ✅ | 97.2% ✅ | 0% ✅ (R9) | — |
| `sell_out_std` | 100,000 | 100.0% ✅ | 100.0% ✅ | 100.0% ✅ | — |
| `nielsen_std` | 362 | — | 100.0% ✅ | — | 100.0% ✅ |
| `mkt_on_std` | 7,282 | 100.0% ✅ | 100.0% ✅ | — | — |
| `mkt_off_std` | 100,000 | 100.0% ✅ | 100.0% ✅ | NULL (R9) ✅ | — |

---

## Validation Assertions

| ID | Rule | Check | Final Status |
|---|---|---|---|
| A1 | R6 | MKT_ON has no UPC join | ✅ PASS |
| A2 | R7 | MKT_OFF has no UPC join | ✅ PASS |
| A3 | R8 | MKT_OFF has no CADENA join | ✅ PASS |
| A4 | R9 | MKT_OFF `cadena_std` = NULL for 100% rows | ✅ PASS |
| A5 | R10 | SELL_OUT 1:1 `mat_idt` per `sku_ean_cod` | ✅ PASS |
| A6 | R10 | Zero EAN→MAT_IDT fanout in `sell_out_std` | ✅ PASS |
| A7 | R11 | No fuzzy matches in any `*_std` output | ✅ PASS |
| A8 | R15 | Row count reconciliation | ✅ PASS |
| A9 | — | NULL key dimensions in quarantine | ✅ PASS (quarantine empty — all rows mapped) |

---

## Architectural Exemptions (Expected NULLs)

| Column | Source | NULL rate | Reason | Reference |
|---|---|---|---|---|
| `cadena_std` | sell_in_std | 100% | SELL_IN ships to CEDIS distribution centers — no retail chain dimension exists at this level (M2 confirmed 2026-06-27) | `phase3_architectural_decisions.yaml` |
| `cadena_std` | mkt_off_std | 100% | Offline media has no chain dimension (R9) | Architectural rule |
| `cadena_std` | mkt_on_std | ~87% | Digital campaigns are brand-level, not chain-level | Architectural rule |

---

## Locked Architectural Rules

| Rule | Description |
|---|---|
| **MAT_IDT** | Unique SAP product key — primary product grain in V_D_ITEM |
| **SKU_EAN_COD** | Barcode attribute — NOT a unique key; maps 1:N to MAT_IDT (history) |
| **R4** | Active catalog filter: `SKU_EAN_COD IS NOT NULL` — never `MAT_ACT_FLG = 1` |
| **R5** | UPC/EAN bridge is for product ID linking only — never a cross-source join predicate |
| **R6** | MKT_ON must never join by UPC/EAN |
| **R7** | MKT_OFF must never join by UPC/EAN |
| **R8** | MKT_OFF must never join by CADENA |
| **R9** | Structural NULLs in `cadena_std` for SELL_IN and MKT_OFF are correct and expected |
| **R10** | SELL_OUT EAN dedup: `MIN(MAT_IDT) GROUP BY EAN` — guarantees 1:1 at output |
| **R11** | Fuzzy matches: quarantine-only — PROHIBITED from auto-promotion to `*_std` |
| **R12** | All mapping rules from YAML/CSV — never hardcoded in SQL |
| **R13** | `nielsen_std` must be unique on `MRKT_DSC_SHRT` before any fact join |
| **R14** | `load_mapping_csv()` asserts key uniqueness before every join |
| **R15** | `assert_row_count_exact()` after every left join — no silent fan-out allowed |

---

## Critical Engineering Notes (Lessons from Phase 3 Validation)

These issues were discovered and resolved during the iterative validation runs (2026-06-27). Document them to prevent recurrence.

### 1. Spark column ambiguity in mapping CSV joins (W4/W5/W6/W7)

**Problem:** When a mapping CSV column shares a name with a column already on the left DataFrame, the post-join `F.col("col_name")` resolves to the left frame's version (which may be NULL or wrong).

**Symptom:** `canal_std`, `cadena_std`, `region_std` showing 0% coverage after join despite a correct mapping file.

**Fix pattern applied in `silver_sell_out.py` and `silver_nielsen.py`:**
```python
# 1. Alias mapping columns before join
df_mapping = df_mapping.select(
    F.col("key").alias("_map_key"),
    F.col("canal_std").alias("canal_std_mapped"),
    F.col("cadena_std").alias("cadena_std_mapped"),
)
# 2. Join
df_result = df_fact.join(df_mapping, df_fact["key_col"] == df_mapping["_map_key"], "left")
# 3. Resolve back to canonical name
df_result = df_result.withColumn("canal_std", F.col("canal_std_mapped"))
```

### 2. Spark case-insensitive column resolution (nielsen `region_std`)

**Problem:** `signoff_03_nielsen_markets.csv` contained two region columns:
- `REGION_STD` (col 7, uppercase, **empty** — from original Snowflake signoff)
- `region_std` (col 10, lowercase, **populated** — from M1 classify script)

Spark's case-insensitive resolution always picked col 7 (the empty one). `df.drop("REGION_STD")` also dropped both columns (case-insensitive).

**Fix:** Load the mapping CSV via **pandas** (case-sensitive) and select columns by exact name, then convert to Spark:
```python
import pandas as pd
_pdf = pd.read_csv(path, dtype=str).fillna("")
_keep = {
    "mrkt_key":      _pdf["MRKT_DSC_SHRT"].str.strip().str.upper(),
    "canal_std_m1":  _pdf["canal_std"].str.strip(),    # col 9 — exact lowercase
    "region_std_m1": _pdf["region_std"].str.strip(),   # col 10 — exact lowercase
    "mapping_status": _pdf["REVIEW_STATUS"].str.strip(),
}
df_m1_sel = spark.createDataFrame(pd.DataFrame(_keep))
```

### 3. VW_FACT_SELL_OUT.UPC ≠ VW_D_PRODUCT_RM.UPC

**Problem:** `sell_out.so_brand = 100% NULL` after joining `VW_FACT_SELL_OUT` with `VW_D_PRODUCT_RM` on `UPC = UPC`.

**Root cause:** `VW_FACT_SELL_OUT.UPC` is an **internal product code** that equals `VW_D_PRODUCT_RM.INT_ID`, not `VW_D_PRODUCT_RM.UPC` (which is the EAN barcode). The `upc=upc` join produced zero matches.

**Fix:**
```python
df_so = df_so.join(
    df_product.select(F.col("sell_out_int_id").alias("upc_key"), ...),
    df_so["upc"] == F.col("upc_key"),  # FACT.UPC = PRODUCT.INT_ID
    "left")
```

### 4. M1 key case normalization (W6)

**Problem:** `signoff_03_nielsen_markets.csv` keys were mixed-case (`"Autos Scanning Area 1"`) but `MRKT_DSC_SHRT` from Snowflake arrives uppercase (`"AUTOS SCANNING AREA 1"`). Join produced zero matches.

**Fix:** Normalize both sides before join:
```python
# Mapping side — in pandas load
"mrkt_key": _pdf["MRKT_DSC_SHRT"].str.strip().str.upper()

# Fact side — in Spark join condition
F.upper(F.trim(df_nielsen_dim["MRKT_DSC_SHRT"])) == df_m1_sel["mrkt_key"]
```

---

## Tech Debt

See `configs/tech_debt.yaml` for full tracking.

| ID | Risk | Severity | Deadline |
|---|---|---|---|
| **TD-006** | Snowflake password `PRD_OSM_DPH_READER` committed to git history | **🔴 HIGH** | **2026-07-01** |
| **TD-001** | Nielsen `CTE_PERIOD` hardcoded through 2026-12 — breaks Jan 2027 | 🟡 MEDIUM | 2026-10-01 |
| — | Date filters (`BIL_DAT >= 20250101`) hardcoded in all silver notebooks | 🟡 MEDIUM | Before Phase 4 |
| — | Brand CASE crosswalk duplicated in MKT_ON, MKT_OFF, SELL_IN | 🟡 MEDIUM | Before Phase 4 |
| — | Coverage thresholds inline in validation notebook — move to `dq_thresholds.yaml` | 🟢 LOW | Before Phase 4 |
| — | Silver stub notebooks (forecast, inventory, waste, price, promotions, investment) | 🟢 LOW | Phase 4 scope |
