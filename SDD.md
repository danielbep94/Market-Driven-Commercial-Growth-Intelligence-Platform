# Software Design Document — Market Driven Commercial Growth Intelligence Platform

**Version:** 1.0  
**Date:** 2026-06-27  
**Author:** MDM Project (Victor Hernandez)  
**Status:** Phase 3 ACTIVE

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
                                     phase3_mdm_validation.py
                                              ↓
                                     Phase 4 Gold Layer
```

**Key principle:** Snowflake tables are read-only source-of-truth. No writes to Snowflake in Phase 3.

---

## 2. Data Sources

| Source | System | Database.Schema | Primary Fact | Grain |
|---|---|---|---|---|
| SELL_IN | Snowflake/SAP | PRD_MEX.MEX_DSP_OTC | VW_FACT_RNV | Transaction (BIL_DAT) |
| SELL_OUT | Snowflake | PRD_MDP.MDP_DSP | VW_FACT_SELL_OUT | Transaction (PER_ID) |
| MKT_ON | Snowflake | PRD_MDP.MDP_DSP | VW_MKT_ECOMM | Campaign row (ANIO) |
| MKT_OFF | Snowflake | PRD_MDP.MDP_STG | FACT_MEDIA_OFF | Campaign row (ANIO) |
| NIELSEN | Snowflake | PRD_MEX.MEX_DSP_DPH_MKT | VW_*_NLSN_AGG_DATA_PVT | Market × Period × Metric |

### Snowflake Credentials
- PRD_MEX: `configs/snowflake_creds.py` → `SF_MEX_USER` / `SF_MEX_PASSWORD`
- PRD_MDP: `configs/snowflake_creds.py` → `SF_MDP_USER` / `SF_MDP_PASSWORD` (fallback: Databricks secret scope `DAN-AM-P-KVT800-R-MDP-DB`)

---

## 3. Standardization Rules (Locked — Phase 2 Approved)

### Product Identity
| Rule | Description |
|---|---|
| MAT_IDT | Unique SAP product key — primary product grain |
| SKU_EAN_COD | Barcode attribute — NOT unique, may map to multiple MAT_IDT |
| R4 | Active catalog filter: `WHERE SKU_EAN_COD IS NOT NULL` — never `MAT_ACT_FLG = 1` |

### Join Safety
| Rule | Description |
|---|---|
| R5 | UPC/EAN bridge is for product ID linking only — never a cross-source join predicate |
| R6 | MKT_ON never joins by UPC/EAN |
| R7 | MKT_OFF never joins by UPC/EAN |
| R8 | MKT_OFF never joins by CADENA |
| R13 | nielsen_std must be unique on MRKT_DSC_SHRT before any fact join |
| R14 | `load_mapping_csv()` asserts key uniqueness before every join |
| R15 | `assert_row_count_exact()` after every left join — no silent fan-out |

### Structural NULLs (R9)
| Field | Source | Value | Reason |
|---|---|---|---|
| `cadena_std` | SELL_IN | NULL | Ships to CEDIS — no chain granularity (M2 confirmed) |
| `cadena_std` | MKT_OFF | NULL | Offline media has no chain dimension |
| `cadena_std` | MKT_ON | NULL (86.89%) | Digital campaigns are brand-level |

### Fuzzy Matching
| Rule | Description |
|---|---|
| R11 | Fuzzy matches are quarantine-only — PROHIBITED from auto-promotion to *_std |

---

## 4. Configuration-Driven Design

All mapping rules come from YAML or CSV configuration (Rule R12). No business logic hardcoded in SQL.

| Config file | Purpose |
|---|---|
| `configs/brand_crosswalk.yaml` | MARCA variant → marca_std (v2.0, Phase C.5) |
| `configs/pipeline_config.yaml` | Source table paths, environments, schedules |
| `configs/dq_thresholds.yaml` | All DQ thresholds |
| `configs/phase3_architectural_decisions.yaml` | Structural NULL decisions (R9) |
| `logs/signoff_03_nielsen_markets.csv` | M1: 362 Nielsen market strings → canal_std / region_std |
| `logs/signoff_05_store_chain_classification.csv` | M3: 19 retail chains → cadena_std |
| `logs/signoff_05_store_format_classification.csv` | M4: 86 store formats → canal_std |

### Known Config Violations (Tech Debt)
- Date filters (`BIL_DAT >= 20250101`, `ANIO >= 2024`) are hardcoded in silver notebooks — must move to `pipeline_config.yaml`
- Brand CASE crosswalk duplicated 3 times in MKT_ON/MKT_OFF — must move to a single CSV
- Coverage thresholds inline in `phase3_mdm_validation.py` — must move to `dq_thresholds.yaml`

---

## 5. Output Datasets

All outputs written to `logs/` (repo) and `dbfs:/mnt/mdp/mdm/phase3_std/` (DBFS).

| Dataset | File | Rows | Key Columns |
|---|---|---|---|
| sell_in_std | `logs/sell_in_std.csv` | 49,815 | mat_idt, marca_std, canal_std, cadena_std (NULL) |
| sell_out_std | `logs/sell_out_std.csv` | ~100K+ | mat_idt, sku_ean_cod, cadena_std, canal_std |
| nielsen_std | `logs/nielsen_std.csv` | 362 | MRKT_DSC_SHRT, canal_std, region_std |
| nielsen_facts_std | `logs/nielsen_facts_std.csv` | 1,092,556 | CBU, MRKT_DSC_SHRT, period, fact metrics |
| mkt_on_std | `logs/mkt_on_std.csv` | 7,282 | marca_std, canal_std, cadena_std (86.89% NULL) |
| mkt_off_std | `logs/mkt_off_std.csv` | ~100K+ | marca_std, canal_std, cadena_std (100% NULL) |

---

## 6. Validation Strategy

`phase3_mdm_validation.py` runs **9 structural assertions** (A1–A9):

| ID | Rule | Type |
|---|---|---|
| A1 | R6 — MKT_ON no UPC join | Hard blocker |
| A2 | R7 — MKT_OFF no UPC join | Hard blocker |
| A3 | R8 — MKT_OFF no CADENA join | Hard blocker |
| A4 | R9 — MKT_OFF cadena_std = 100% NULL | Hard blocker |
| A5 | R10 — SELL_OUT bridge columns present | Hard blocker |
| A6 | R10 — Zero EAN fanout | Hard blocker |
| A7 | R11 — No fuzzy matches in any *_std | Hard blocker |
| A8 | R15 — Row count reconciliation | Warning only |
| A9 | — NULL accountability (quarantine check) | Hard blocker (if unaccounted) |

Plus 12 dimension coverage checks (PASS/BELOW_THRESHOLD, not blockers).

**Gate output:** `PHASE 3 GATE: {CLEAR|BLOCKED|BLOCKED_N_ERRORS}`

---

## 7. Audit Logging Strategy

Every silver notebook and the validation notebook log to a shared audit log via `silver_homologation_apply.py` helpers:

```python
log("INFO",    "message", section)     # informational
log("WARNING", "message", section)    # non-blocking
blocker(condition, "message", section) # hard blocker — added to _HARD_BLOCKERS list
passed("message", section)             # assertion passed
```

**Audit log location:**
- DBFS: `dbfs:/mnt/mdp/mdm/phase3_std/phase3_standardization_audit_log.txt`
- Repo: `logs/phase3_standardization_audit_log.txt`

**Quarantine log:**
- DBFS: `dbfs:/mnt/mdp/mdm/phase3_std/phase3_quarantine_report.txt`
- Repo: `logs/phase3_quarantine_report.txt`

All quarantined rows must be accounted for by A9. Silent drops are a hard blocker.

---

## 8. Error Handling Rules

| Pattern | Handling |
|---|---|
| Snowflake inaccessible | `blocker()` — stops downstream joins, writes NULL output |
| Missing mapping CSV | `warn()` — returns empty DataFrame (risk: silent NULL output) |
| CBU table read failure (Nielsen) | Currently `warn()` — **should be `blocker()`** (known risk) |
| Row count fan-out after join | `blocker()` via `assert_row_count_exact()` |
| Fuzzy match in output | `blocker()` via A7 |
| NULL key dim not in quarantine | `blocker()` via A9 |

---

## 9. Promotion Criteria (Phase 3 → Phase 4)

Phase 3 outputs are eligible for Phase 4 Gold promotion when:

1. ✅ `phase3_mdm_validation.py` runs with 0 hard blockers (`PHASE 3 GATE: CLEAR`)
2. ✅ All M1–M4 manual mappings are `CONFIRMED` (not `NEEDS_REVIEW`)
3. ✅ JOIN_REGISTRY shows no prohibited join patterns (A1/A2/A3)
4. ✅ `phase3_null_rate_validation.txt` confirms NULL accountability
5. ✅ `phase3_mapping_coverage_report.txt` meets all dimension thresholds
6. ✅ `phase3_quarantine_report.txt` documents all excluded rows
7. ✅ `phase3_row_count_reconciliation.txt` matches expected source counts
8. ⚠️ Tech debt items TD-001 and TD-006 should be reviewed before Phase 4 Gold promotion

---

## 10. Known Risks and Tech Debt

| ID | Risk | Severity | Deadline |
|---|---|---|---|
| TD-001 | Nielsen CTE_PERIOD hardcoded through 2026-12 | MEDIUM | 2026-10-01 |
| TD-006 | Git history contains PRD_OSM_DPH_READER credentials | **HIGH** | **2026-07-01** |
| — | Brand CASE crosswalk duplicated 3 times | MEDIUM | Before Phase 4 |
| — | Date filters hardcoded in all silver notebooks | MEDIUM | Before Phase 4 |
| — | CBU failure treated as warning in silver_nielsen | MEDIUM | Immediate |
| — | JOIN_REGISTRY mutation without try/finally guard | LOW | Before Phase 4 |
