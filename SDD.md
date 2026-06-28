# Software Design Document — Market Driven Commercial Growth Intelligence Platform

**Version:** 3.0  
**Date:** 2026-06-27  
**Author:** MDM Project (Victor Hernandez)  
**Status:** 🟡 Phase 4 Gold KPI — IN PROGRESS — commit 187fc77

---

## 1. Architecture Overview

The platform follows a Medallion Architecture (Bronze → Silver → Gold → Mart) implemented across Databricks (PySpark) and Snowflake (source-of-truth).

```
Snowflake (READ-ONLY)                 Databricks
────────────────────                  ──────────
PRD_MEX.MEX_DSP_OTC     ─── read ──▶ Phase 3 Silver Notebooks
PRD_MEX.MEX_DSP_DPH_MKT ─── read ──▶  silver_sell_in.py
PRD_MDP.MDP_DSP         ─── read ──▶  silver_sell_out.py
PRD_MDP.MDP_STG         ─── read ──▶  silver_nielsen.py
                                       silver_mkt_on_std.py
                                       silver_mkt_off_std.py
                                              ↓
                                     logs/*_std.csv (DBFS + repo)
                                              ↓
                                     phase3_mdm_validation.py  ← 🟢 GATE: CLEAR
                                              ↓
                                     Phase 4 Gold Layer  ← APPROVED
```

**Key principle:** Snowflake tables are read-only source-of-truth. No writes to Snowflake in any phase.

---

## 2. Data Sources

| Source | System | Database.Schema | Primary Fact Table | Grain |
|---|---|---|---|---|
| SELL_IN | Snowflake / SAP | PRD_MEX.MEX_DSP_OTC | VW_FACT_RNV | Transaction (BIL_DAT) |
| SELL_OUT | Snowflake | PRD_MDP.MDP_DSP | VW_FACT_SELL_OUT | Transaction (PER_ID) |
| MKT_ON | Snowflake | PRD_MDP.MDP_DSP | VW_MKT_ECOMM | Campaign row (ANIO) |
| MKT_OFF | Snowflake | PRD_MDP.MDP_STG | FACT_MEDIA_OFF | Campaign row (ANIO) |
| NIELSEN | Snowflake | PRD_MEX.MEX_DSP_DPH_MKT | VW_*_NLSN_AGG_DATA_PVT | Market × Period × Metric |

### Key Dimension Tables

| Table | Source | Purpose |
|---|---|---|
| `PRD_MEX.MEX_DSP_OTC.V_D_ITEM` | Snowflake | Product master — MAT_IDT + SKU_EAN_COD |
| `PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM` | Snowflake | SELL_OUT product dim — INT_ID, UPC (EAN), BRAND |
| `PRD_MDP.MDP_DSP.VW_D_STORE_RM` | Snowflake | SELL_OUT store dim — CHAIN, FORMAT |
| `PRD_MEX.MEX_DSP_OTC.V_D_CLIENT` | Snowflake | SELL_IN client dim — CUS_GRN_CHL_DSC (canal) |

### Snowflake Credentials
- PRD_MEX: `configs/snowflake_creds.py` → `SF_MEX_USER` / `SF_MEX_PASSWORD`
- PRD_MDP: `configs/snowflake_creds.py` → `SF_MDP_USER` / `SF_MDP_PASSWORD`  
  (fallback: Databricks secret scope `DAN-AM-P-KVT800-R-MDP-DB`)

---

## 3. Standardization Rules (Locked — Phase 2 Approved)

### Product Identity

| Rule | Description |
|---|---|
| **MAT_IDT** | Unique SAP product key — primary product grain in V_D_ITEM |
| **SKU_EAN_COD** | Barcode attribute — NOT unique; one EAN may map to multiple MAT_IDTs (inactive history) |
| **R4** | Active catalog filter: `WHERE SKU_EAN_COD IS NOT NULL` — never `MAT_ACT_FLG = 1` |

### Join Safety

| Rule | Description |
|---|---|
| R5 | UPC/EAN bridge is for product ID linking only — never a cross-source join predicate |
| R6 | MKT_ON must never join by UPC/EAN |
| R7 | MKT_OFF must never join by UPC/EAN |
| R8 | MKT_OFF must never join by CADENA |
| R10 | SELL_OUT EAN dedup: `MIN(MAT_IDT) GROUP BY EAN` — guarantees 1:1 at output |
| R13 | `nielsen_std` must be unique on `MRKT_DSC_SHRT` before any fact join |
| R14 | `load_mapping_csv()` asserts key uniqueness before every join |
| R15 | `assert_row_count_exact()` after every left join — no silent fan-out |

### Structural NULLs (R9) — Architectural Decisions

| Column | Source | NULL rate | Reason | Confirmed |
|---|---|---|---|---|
| `cadena_std` | sell_in_std | 100% | SELL_IN ships to CEDIS distribution centers — no retail chain dimension at this supply chain level | M2 discovery 2026-06-27 |
| `cadena_std` | mkt_off_std | 100% | Offline media buys are not tied to specific chains | Architectural rule |
| `cadena_std` | mkt_on_std | ~87% | Digital campaigns are brand-level, not chain-level | Architectural rule |

These are **PASS** in validation with `ARCHITECTURAL_EXEMPTION` label. Threshold set to 0% for sell_in.

### Fuzzy Matching

| Rule | Description |
|---|---|
| R11 | Fuzzy matches are quarantine-only — PROHIBITED from auto-promotion to `*_std` outputs |
| R12 | All mapping rules from YAML/CSV config — never hardcoded in SQL or Python |

---

## 4. Mapping System (M1–M4)

All four manual mapping dimensions are **CONFIRMED** as of 2026-06-27.

| ID | Dimension | Source column | Output column | Mapping file | Status |
|---|---|---|---|---|---|
| M1 | Nielsen market → canal + region | `MRKT_DSC_SHRT` | `canal_std`, `region_std` | `logs/signoff_03_nielsen_markets.csv` | ✅ 362/362 CONFIRMED |
| M2 | SELL_IN cadena discovery | `V_D_CLIENT.*` | `cadena_std` | N/A — structural NULL (R9) | ✅ CONFIRMED NULL |
| M3 | SELL_OUT chain → cadena | `VW_D_STORE_RM.CHAIN` | `cadena_std` | `logs/signoff_05_store_chain_classification.csv` | ✅ 19/19 CONFIRMED |
| M4 | SELL_OUT format → canal | `VW_D_STORE_RM.FORMAT` | `canal_std` | `logs/signoff_05_store_format_classification.csv` | ✅ 86/86 CONFIRMED |

### M1 Classification Script
`scripts/m1_nielsen_classify.py` — runs locally (not in Databricks). Produces the enriched `signoff_03_nielsen_markets.csv` with `canal_std` and `region_std` columns appended. See `scripts/README.md`.

---

## 5. Configuration-Driven Design

All mapping rules come from YAML or CSV configuration (Rule R12). No business logic hardcoded in SQL.

| Config file | Purpose |
|---|---|
| `configs/brand_crosswalk.yaml` | MARCA variant → `marca_std` |
| `configs/pipeline_config.yaml` | Source table paths, environments, schedules |
| `configs/dq_thresholds.yaml` | Coverage thresholds per dimension |
| `configs/phase3_architectural_decisions.yaml` | Structural NULL decisions (R9/M2) |
| `configs/tech_debt.yaml` | All tracked tech debt items |
| `logs/signoff_03_nielsen_markets.csv` | M1: 362 Nielsen market strings → canal_std / region_std |
| `logs/signoff_05_store_chain_classification.csv` | M3: 19 CHAIN values → cadena_std |
| `logs/signoff_05_store_format_classification.csv` | M4: 86 FORMAT values → canal_std |

---

## 6. Output Datasets (Phase 3 — Final Validated State 2026-06-27)

All outputs written to `logs/` (repo) and `dbfs:/mnt/mdp/mdm/phase3_std/` (DBFS).

| Dataset | File | Rows | marca_std | canal_std | cadena_std | region_std |
|---|---|---|---|---|---|---|
| sell_in_std | `logs/sell_in_std.csv` | 49,815 | 97.2% | 97.2% | 0% (R9 ✅) | — |
| sell_out_std | `logs/sell_out_std.csv` | 100,000 | 100.0% | 100.0% | 100.0% | — |
| nielsen_std | `logs/nielsen_std.csv` | 362 | — | 100.0% | — | 100.0% |
| nielsen_facts_std | `logs/nielsen_facts_std.csv` | ~1M | — | — | — | — |
| mkt_on_std | `logs/mkt_on_std.csv` | 7,282 | 100.0% | 100.0% | ~87% NULL | — |
| mkt_off_std | `logs/mkt_off_std.csv` | 100,000 | 100.0% | 100.0% | 100% NULL (R9 ✅) | — |

---

## 7. Validation Strategy

`phase3_mdm_validation.py` runs **9 structural assertions (A1–A9)** and **12 dimension coverage checks**.

### Assertions

| ID | Rule | Type | Final result |
|---|---|---|---|
| A1 | R6 — MKT_ON no UPC join | Hard blocker | ✅ PASS |
| A2 | R7 — MKT_OFF no UPC join | Hard blocker | ✅ PASS |
| A3 | R8 — MKT_OFF no CADENA join | Hard blocker | ✅ PASS |
| A4 | R9 — MKT_OFF cadena_std = 100% NULL | Hard blocker | ✅ PASS |
| A5 | R10 — SELL_OUT 1:1 mat_idt per sku_ean_cod | Hard blocker | ✅ PASS |
| A6 | R10 — Zero EAN fanout in sell_out_std | Hard blocker | ✅ PASS |
| A7 | R11 — No fuzzy matches in any `*_std` | Hard blocker | ✅ PASS |
| A8 | R15 — Row count reconciliation | Warning | ✅ PASS |
| A9 | NULL accountability (quarantine cross-check) | Hard blocker | ✅ PASS — quarantine empty |

### Coverage Thresholds

| Column | Threshold | Final result |
|---|---|---|
| `sell_in_std.marca_std` | 90% | ✅ 97.2% |
| `sell_in_std.canal_std` | 95% | ✅ 97.2% |
| `sell_in_std.cadena_std` | 0% (ARCHITECTURAL_EXEMPTION R9/M2) | ✅ PASS |
| `sell_out_std.marca_std` | 90% | ✅ 100.0% |
| `sell_out_std.canal_std` | 70% | ✅ 100.0% |
| `sell_out_std.cadena_std` | 70% | ✅ 100.0% |
| `mkt_on_std.canal_std` | 50% | ✅ 100.0% |
| `mkt_on_std.marca_std` | 90% | ✅ 100.0% |
| `mkt_off_std.canal_std` | 50% | ✅ 100.0% |
| `mkt_off_std.marca_std` | 90% | ✅ 100.0% |
| `nielsen_std.canal_std` | 50% | ✅ 100.0% |
| `nielsen_std.region_std` | 50% | ✅ 100.0% |

**Gate confirmed:** `PHASE 3 GATE: 🟢 CLEAR` — 2026-06-27 17:37 UTC, commit `e3bbf81`

---

## 8. Audit Logging Strategy

Every silver notebook and the validation notebook log through helpers in `silver_homologation_apply.py`:

```python
log("INFO",    "message", section)      # Informational
warn(cond,     "message", section)      # Non-blocking warning
blocker(cond,  "message", section)      # Hard blocker — appended to _HARD_BLOCKERS
passed(        "message", section)      # Assertion passed
```

| Log file | DBFS path | Repo path |
|---|---|---|
| Primary audit log | `dbfs:/mnt/mdp/mdm/phase3_std/phase3_standardization_audit_log.txt` | `logs/phase3_standardization_audit_log.txt` |
| Coverage report | `dbfs:/mnt/mdp/mdm/phase3_std/phase3_mapping_coverage_report.txt` | `logs/phase3_mapping_coverage_report.txt` |
| NULL rate log | `dbfs:/mnt/mdp/mdm/phase3_std/phase3_null_rate_validation.txt` | `logs/phase3_null_rate_validation.txt` |
| Row count log | `dbfs:/mnt/mdp/mdm/phase3_std/phase3_row_count_reconciliation.txt` | `logs/phase3_row_count_reconciliation.txt` |
| Quarantine report | `dbfs:/mnt/mdp/mdm/phase3_std/phase3_quarantine_report.csv` | **DBFS only** — runtime artifact, never committed |

> **Note on quarantine file:** `phase3_quarantine_report.csv` is written by silver notebooks at runtime. Its absence from the repo `logs/` is expected and emits `INFO` (not a warning) in `phase3_mdm_validation.py`.

---

## 9. Critical Engineering Notes — Lessons Learned (Phase 3 Validation, 2026-06-27)

These issues were discovered and resolved during iterative validation runs. **Document permanently to prevent recurrence.**

### 9.1 Spark column ambiguity in mapping CSV joins

**Symptom:** `canal_std`, `cadena_std` showing 0% coverage after a left join with a mapping DataFrame, despite the mapping file being correct and the join key matching.

**Root cause:** When a mapping DataFrame column shares a name with a column already on the left (fact) DataFrame, `F.col("shared_name")` after the join resolves to the **left frame's** value — which may be NULL or wrong.

**Mandatory fix pattern:**
```python
# 1. Alias ALL mapping output columns before the join
df_map = df_map.select(
    F.col("key_col").alias("_map_key"),
    F.col("canal_std").alias("canal_std_mapped"),
    F.col("cadena_std").alias("cadena_std_mapped"),
)
# 2. Join
df = df_fact.join(df_map, df_fact["source_key"] == F.col("_map_key"), "left")
# 3. Resolve back to canonical names
df = df.withColumn("canal_std", F.col("canal_std_mapped")) \
       .withColumn("cadena_std", F.col("cadena_std_mapped"))
```
**Applied in:** `silver_sell_out.py` (M3/M4), `silver_nielsen.py` (M1)

---

### 9.2 Spark case-insensitive resolution with duplicate column names

**Symptom:** `nielsen_std.region_std = 0%` after join even with correct mapping data and working join key.

**Root cause:** `signoff_03_nielsen_markets.csv` contains **two** region columns:
- `REGION_STD` — col[7], **uppercase**, **empty** (original Snowflake signoff output)
- `region_std` — col[10], **lowercase**, **populated** (added by M1 classify script)

Spark's case-insensitive column resolution picks col[7] for any reference to `region_std`. Additionally, `DataFrame.drop("REGION_STD")` is also case-insensitive and drops **both** columns simultaneously — making `drop()` an invalid workaround.

**Fix — load via pandas (case-sensitive), select by exact column name, convert to Spark:**
```python
import pandas as pd
_pdf = pd.read_csv(path, dtype=str).fillna("")
_keep = {
    "mrkt_key":      _pdf["MRKT_DSC_SHRT"].str.strip().str.upper(),
    "canal_std_m1":  _pdf["canal_std"].str.strip(),    # col[9] lowercase — unambiguous
    "region_std_m1": _pdf["region_std"].str.strip(),   # col[10] lowercase — unambiguous
    "mapping_status": _pdf["REVIEW_STATUS"].str.strip(),
}
df_m1_sel = spark.createDataFrame(pd.DataFrame(_keep))
```
**Applied in:** `silver_nielsen.py` (Step B — M1 mapping load)

---

### 9.3 VW_FACT_SELL_OUT.UPC ≠ VW_D_PRODUCT_RM.UPC

**Symptom:** `sell_out_std.so_brand = 100% NULL` and therefore `marca_std = 0%`.

**Root cause:** `VW_FACT_SELL_OUT.UPC` is an **internal product code** that corresponds to `VW_D_PRODUCT_RM.INT_ID`. `VW_D_PRODUCT_RM.UPC` stores the **EAN barcode**. Joining on `upc = upc` produces zero matches.

**Fix:**
```python
df_so = df_so.join(
    df_product.select(
        F.col("sell_out_int_id").alias("upc_key"),   # INT_ID = FACT.UPC
        F.col("so_name"), F.col("so_brand"), F.col("so_category"), F.col("CBU_ID")
    ),
    df_so["upc"] == F.col("upc_key"),
    "left"
)
```
**Applied in:** `silver_sell_out.py` (Phase C — product enrichment)

---

### 9.4 M1 join key case mismatch

**Symptom:** `nielsen_std.canal_std = 0%` — join produces zero matches despite correct mapping file.

**Root cause:** `signoff_03_nielsen_markets.csv` keys are mixed-case (e.g. `"Autos Scanning Area 1"`) but `MRKT_DSC_SHRT` from Snowflake is uppercase (`"AUTOS SCANNING AREA 1"`).

**Fix — normalize both sides:**
```python
# Mapping side (pandas load): already UPPER via str.upper()
"mrkt_key": _pdf["MRKT_DSC_SHRT"].str.strip().str.upper()

# Fact side (Spark join condition):
F.upper(F.trim(df_nielsen_dim["MRKT_DSC_SHRT"])) == df_m1_sel["mrkt_key"]
```
**Applied in:** `silver_nielsen.py` (Step B — join condition)

---

## 10. Error Handling Rules

| Pattern | Handling |
|---|---|
| Snowflake inaccessible | `blocker()` — stops downstream joins |
| Missing mapping CSV | `warn()` — returns empty DataFrame (risk: silent NULL output) |
| Mapping CSV column ambiguity | Use pandas load (case-sensitive) — see §9.2 |
| CBU table read failure (Nielsen) | Currently `warn()` — **should be `blocker()`** (tech debt) |
| Row count fan-out after join | `blocker()` via `assert_row_count_exact()` |
| Fuzzy match in output | `blocker()` via A7 |
| NULL key dim not in quarantine | `blocker()` via A9 |

---

## 11. Promotion Criteria (Phase 3 → Phase 4)

All criteria met as of **2026-06-27 17:37 UTC**:

| # | Criterion | Status |
|---|---|---|
| 1 | `phase3_mdm_validation.py` — 0 hard blockers, GATE: CLEAR | ✅ Confirmed |
| 2 | M1–M4 mappings all CONFIRMED | ✅ 362+19+86 CONFIRMED |
| 3 | JOIN_REGISTRY — no prohibited join patterns (A1/A2/A3) | ✅ PASS |
| 4 | `phase3_null_rate_validation.txt` — NULL accountability | ✅ Quarantine empty, all rows mapped |
| 5 | `phase3_mapping_coverage_report.txt` — all thresholds met | ✅ 12/12 PASS |
| 6 | `phase3_row_count_reconciliation.txt` — counts confirmed | ✅ All 5 sources |
| 7 | Structural NULLs documented and exempted | ✅ R9/M2 in `phase3_architectural_decisions.yaml` |
| 8 | TD-001 and TD-006 reviewed before Gold promotion | ⚠️ Action required |

---

## 12. Known Risks and Tech Debt

| ID | Risk | Severity | Deadline | Notes |
|---|---|---|---|---|
| **TD-006** | Snowflake password `PRD_OSM_DPH_READER` in git history | 🔴 **HIGH** | **2026-07-01** | Rotate credential immediately |
| **TD-001** | Nielsen `CTE_PERIOD` hardcoded through 2026-12 | 🟡 MEDIUM | 2026-10-01 | Will silently break in Jan 2027 |
| — | Date filters (`BIL_DAT >= 20250101`) hardcoded in silver notebooks | 🟡 MEDIUM | Before Phase 4 | Move to `pipeline_config.yaml` |
| — | Brand CASE crosswalk duplicated in MKT_ON, MKT_OFF, SELL_IN | 🟡 MEDIUM | Before Phase 4 | Consolidate to single CSV |
| — | Coverage thresholds inline in validation notebook | 🟢 LOW | Before Phase 4 | Move to `dq_thresholds.yaml` |
| — | CBU table failure treated as `warn()` in `silver_nielsen.py` | 🟡 MEDIUM | Immediate | Should be `blocker()` |
| — | Silver stub notebooks (forecast, inventory, waste, price, promotions, investment) | 🟢 LOW | Phase 4 scope | Empty stubs only |
| — | JOIN_REGISTRY mutation without try/finally guard | 🟢 LOW | Before Phase 4 | Risk of registry corruption on error |

---

## 13. Gold Layer Design

### 13.1 Architecture Rule

> **Phase 4 performs ZERO Snowflake writes.**  
> All Gold outputs land on `dbfs:/mnt/mdp/mdm/phase4_gold/data/` (compute) and `logs/` (audit, small files only).  
> B14 + B15 pre-confirmed 2026-06-27: zero Snowflake write or mutation statements in any Phase 4 notebook.

### 13.2 Input Files (Phase 3 Silver — Gate CLEAR commit e3bbf81)

| File | Rows | Status |
|---|---|---|
| `logs/sell_in_std.csv` | 49,815 | ✅ Full period |
| `logs/sell_out_std.csv` | 100,000 | ⚠️ Possible sample cap |
| `logs/mkt_on_std.csv` | 7,282 | ✅ Full period |
| `logs/mkt_off_std.csv` | 100,000 | ✅ Full period |
| `logs/nielsen_std.csv` | 362 | ✅ Market dim |
| `logs/nielsen_facts_std.csv` | 100,001 | ✅ Confirmed Phase 3 output |

### 13.3 Master Join Strategy

**Master grain:** `fecha_month × marca_std × canal_std × cadena_std`

**Fan-out prevention (F1):** All right-side tables must be aggregated to master-safe grain before joining.

```
gold_sell_out_kpi               ← BASE (finest grain)
  LEFT JOIN gold_sell_in_kpi_master   ON fecha_month, marca_std, canal_std
  LEFT JOIN gold_investment_kpi       ON fecha_month, marca_std, canal_std  [Danone only]
  LEFT JOIN gold_nielsen_kpi_master   ON fecha_month, canal_std
```

Before every join:
1. `assert_unique_keys(right_df, join_keys, table_name)` — B10
2. `assert_no_join_fanout(base_count, joined_df, join_name)` — B8
3. Log pre-join and post-join row counts

### 13.4 Dimensions Excluded from Master Table (F2)

- `region_std` — Nielsen regional detail. Available in `gold_nielsen_kpi.csv` only.
- `cbu` — SELL_IN CBU detail. Available in `gold_sell_in_kpi.csv` only.
- These must NOT be added to `gold_commercial_kpi` unless the master grain is formally expanded.

### 13.5 Brand Owner Classification (F6)

```python
brand_owner_type = DANONE    # brand in brand_crosswalk.yaml danone_brands
brand_owner_type = COMPETITOR # all others
```
- Source: `configs/brand_crosswalk.yaml` (danone_brands keys)
- `gold_commercial_kpi` includes Danone brands only
- Competitor rows retained in `gold_investment_kpi.csv` for benchmark analysis

### 13.6 Date Range

```python
GOLD_START_MONTH = "2025-01-01"
GOLD_END_MONTH   = None  # auto-detect from Silver max(fecha_month)
RUN_MODE         = "FULL"  # FULL | SAMPLE
```

- `fecha_month` = `F.trunc(date_col, "MM")` in every Gold notebook (B13)
- B11: rows outside date range are hard-blocked
- B12: SAMPLE mode blocks Gold output in production

---

## 14. KPI Definitions

### 14.1 SELL_IN KPIs

| KPI | Formula | Unit | Source column |
|---|---|---|---|
| `si_revenue_mxn` | `SUM(REVENUE_MXN)` | MXN | `REVENUE_MXN` |
| `si_vol_litros` | `SUM(VOLUME_LITER)` | Litres | `VOLUME_LITER` |
| `si_vol_kg` | `SUM(VOLUME_KGR)` | KG | `VOLUME_KGR` |
| `si_avg_price_mxn_per_litre` | `si_revenue_mxn / si_vol_litros` | MXN/L | derived |
| `si_avg_price_mxn_per_kg` | `si_revenue_mxn / si_vol_kg` | MXN/KG | derived |
| `si_sku_count` | `COUNT(DISTINCT SKU_EAN_COD)` | # | `SKU_EAN_COD` |
| `si_transaction_count` | `COUNT(*)` | # | — |

### 14.2 SELL_OUT KPIs

| KPI | Formula | Unit | Source column |
|---|---|---|---|
| `so_revenue_mxn` | `SUM(REVENUE_SELL_OUT)` | MXN | `REVENUE_SELL_OUT` |
| `so_vol_units` | `SUM(VOL_SELL_OUT)` | Units | `VOL_SELL_OUT` |
| `so_pcs` | `SUM(PCS_SELL_OUT)` | Pieces | `PCS_SELL_OUT` |
| `so_avg_price_mxn` | `so_revenue_mxn / so_vol_units` | MXN/unit | derived |
| `so_inventory_units` | `SUM(VOL_INV)` | Units | `VOL_INV` |
| `so_inventory_days` | `(so_inventory_units × 30) / so_vol_units` | Days | derived |
| `so_store_count` | `COUNT(DISTINCT STORE_ID)` | # | `STORE_ID` |
| `so_sku_count` | `COUNT(DISTINCT sku_ean_cod)` | # | `sku_ean_cod` |
| `coverage_level` | CASE on store_count vs thresholds | LOW/MED/HIGH | `dq_thresholds.yaml` |

### 14.3 Investment KPIs

| KPI | Formula | Unit | Source |
|---|---|---|---|
| `inv_mkt_on_mxn` | `SUM(INVERSION_REAL)` WHERE MKT_ON | MXN | `INVERSION_REAL` |
| `inv_mkt_off_mxn` | `SUM(INVERSION_REAL)` WHERE MKT_OFF | MXN | `INVERSION_REAL` |
| `inv_total_mxn` | `inv_mkt_on_mxn + inv_mkt_off_mxn` | MXN | derived |
| `inv_on_pct` | `inv_mkt_on_mxn / inv_total_mxn` | % | derived |
| `inv_campaign_count` | `COUNT(DISTINCT CAMPANA)` | # | `CAMPANA` |
| `inv_platform_count` | `COUNT(DISTINCT SOPORTE_PLATAFORMA)` | # | MKT_ON only |
| `inv_media_type_count` | `COUNT(DISTINCT MEDIO)` | # | `MEDIO` |

### 14.4 Nielsen KPIs

| KPI | Source `METRIC_NAME` | Unit |
|---|---|---|
| `nls_units` | `U` | Units |
| `nls_avg_unit_price` | `AVG_U_PRC` | MXN/unit |
| `nls_avg_equiv_price` | `AVG_E_PRC` | MXN/KG |
| `nls_value_share` | `VALUE_SHARE` | % |
| `nls_volume_share` | `VOLUME_SHARE` | % |
| `nls_numeric_dist` | `NUMERIC_DISTRIBUTION` | % |
| `nls_category_value_mxn` | `CATEGORY_VALUE` | MXN |

> Nielsen measures market-level share — no `marca_std` in source. Join to master is on `(fecha_month, canal_std)` only.

### 14.5 Derived KPIs

| KPI | Formula | Note |
|---|---|---|
| `roas_gross` | `so_revenue_mxn / inv_total_mxn` | F5: NOT roi_gross. Guarded: NULL when inv=0 or NULL. |
| `data_confidence` | HIGH / MEDIUM / LOW | Based on source availability per row |

---

## 15. Master Join Strategy Detail

See §13.3. Required utility functions in `gold_kpi_utils.py`:

| Function | Enforces | Description |
|---|---|---|
| `safe_divide(num, den)` | B4 | Returns NULL (not 0/Inf) when denominator is 0 or NULL |
| `assert_unique_keys(df, keys, name)` | B10 | Raises ValueError if not unique |
| `assert_no_join_fanout(base_n, df, name)` | B8 | Raises ValueError if joined count > base |
| `check_run_mode()` | B12 | Raises RuntimeError if RUN_MODE=SAMPLE |
| `check_fecha_month_range(df, name)` | B11+B13 | Checks range and day=1 |
| `check_no_inf_nan(df, cols, name)` | B4 | Checks for Inf/NaN in derived columns |

---

## 16. Data Confidence Logic

```python
data_confidence = (
    HIGH   # all 4 sources present (sell_in, sell_out, investment, nielsen)
    MEDIUM # sell_out present, ≥1 other source missing
    LOW    # sell_out absent
)
```

Source: `gold_commercial_kpi.py` metadata columns.
Inputs: `has_sell_in`, `has_sell_out`, `has_investment`, `has_nielsen` (boolean).

---

## 17. Security — Snowflake No-Write Rule

| Rule | Status |
|---|---|
| No Snowflake writes in Phase 4 | **ENFORCED — B14/B15 PRE-CONFIRMED 2026-06-27** |
| No production Snowflake table mutations | **ENFORCED — B15 PRE-CONFIRMED 2026-06-27** |
| Snowflake used as read-only source in Phases 1–3 only | Confirmed |
| Gold writes: DBFS only (`dbfs:/mnt/mdp/mdm/phase4_gold/data/`) | Confirmed |
| Audit files only in repo `logs/` (small, non-sensitive) | Confirmed |
| Large commercial KPI CSVs: NOT committed to repo | B16 blocker in validation |
| TD-006: `PRD_OSM_DPH_READER` credential rotation | 🔴 HIGH — deferred to project end |

