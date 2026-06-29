# `cat_canal.csv` — Build Documentation
## Danone Master Data Catalog v6.0.0 | 2026-06-29

---

## 1. Objective

`cat_canal.csv` is the **canonical Danone channel dimension** for Mexico,
enabling cross-source comparability of commercial channel data across three
independent source systems:

| Source | System | Table | Role |
|---|---|---|---|
| IBP | `PRD_MDP.MDP_DSP.VW_FACT_DANONE_IBP` | Planning | Commercial planning hierarchy |
| SELL_IN | `PRD_MEX.MEX_DSP_OTC.V_D_CLIENT` | Transactional | Customer channel classification |
| SELL_OUT | `PRD_MDP.MDP_DSP.VW_D_STORE_RM` | Market (Nielsen) | Retail format classification |

**Governing principle:** Business logic first. Danone's global channel taxonomy
(Modern Trade = UTT, Proximity/Traditional = DTT, Central America = CAM) governs
classification — not source system structure or column prefix alone.

**Strict constraint:** Snowflake is **READ-ONLY**. All outputs land on
`dbfs:/mnt/mdp/mdm/master_catalog/canal/` and `logs/`. Zero writes to Snowflake.

---

## 2. Channel Taxonomy (Business Rules)

### Gran Canal Groups

| gran_canal_grp | Business Meaning | Examples |
|---|---|---|
| **UTT** | Unidad de Tiendas — Modern/Organized Trade | Walmart, Soriana, Chedraui, Sam's, Costco, all e-commerce |
| **DTT** | Distribución Tradicional — Proximity/Convenience | OXXO, all 7-Eleven formats, CASH AND CARRY |
| **CAM** | Centro América — IBP planning scope only | IBP export planning channel |
| **UNKNOWN** | Unclassified | SELL_IN `NA` label |
| **PENDING** | Unresolved / requires business review | Live formats not in governed seed |

### Confirmed Business Decisions

| Format / Channel | gran_canal_grp | confirmation_status | Rationale |
|---|---|---|---|
| All Walmart, Soriana, Chedraui, HEB, CCF formats | UTT | CONFIRMED | Centralized commercial agreements, Modern Trade logistics |
| All e-commerce (`*-ECOMMERCE`, RAPPI, CORNERSHOP, PURE PLAYER) | UTT | CONFIRMED | Operated under Modern Trade taxonomy |
| All 7-Eleven formats (`7E-*`) | DTT | CONFIRMED | Convenience/proximity — location variant does not change channel |
| OXXO | DTT | CONFIRMED | Mexico's largest convenience/proximity channel |
| CASH AND CARRY | DTT | CONFIRMED_RECOMMENDED | High-frequency B2B replenishment; not aligned with centralized MT logistics |
| 7E-AEROPUERTO, 7E-AEROPUERTO PREMIUM | DTT | CONFIRMED | Airport location does not change convenience channel nature |
| SO-CITY CLUB, SO-EXPRESS | UTT | CONFIRMED | Soriana organized trade — Modern Trade pricing & negotiation |
| CCF-ECOMMERCE, CX-ECOMMERCE | UTT | CONFIRMED | Centralized MT commercial agreements |
| SUBCHAIN | EXCLUDED | AMBI-07 | 33.33% null rate; not a reliable join key |
| CAM (IBP only) | CAM | CONFIRMED | Central America planning scope; never remapped to UTT/DTT |

> **Note on `CONFIRMED_RECOMMENDED`:** These rows are `promoted = YES` and
> appear in `cat_canal.csv`. The `CONFIRMED_RECOMMENDED` status is informational
> only — it flags that a final business sign-off is recommended but does not
> block promotion.

---

## 3. Source System Hierarchy Per Source

### IBP — 5-Level Commercial Hierarchy
```
GRAN_CANAL → CANAL → GRUPO → CADENA → FM
```
- Filter: `NOMBRE_ETIQUETA = 'REAL'` (actuals only, excludes budget/forecast)
- L1 (`GRAN_CANAL`) and L2 (`CANAL`) → `promoted = YES`
- L3–L5 → `cat_canal_reference.csv` only (reference/lineage)

### SELL_IN — 2-Level Commercial Hierarchy
```
cus_grn_chl_dsc (L1) → lv6_hie_cus_dsc (L2, reference only)
```
- Source: `V_D_CLIENT` (all rows, no `stat_cod` filter — field is unreliable)
- Empty strings treated the same as NULL (excluded from classification)
- `lv6_hie_cus_dsc`: **23,532 distinct values** = individual customer codes
  (grain-level operational data). Demoted to `REFERENCE_ONLY` via
  `distinct > 50` threshold. Not a channel dimension.

### SELL_OUT — 2-Level Retail Hierarchy
```
FORMAT (L1) → CHAIN (L2, reference only)
```
- Source: `VW_D_STORE_RM` (86 live FORMAT values)
- Classification: seed-file driven (`canal_unified_seed.csv`)
- CHAIN: `promoted = NO`, `confirmation_status = REFERENCE_ONLY` — not a canonical
  join key. Multi-format chains get `gran_canal_grp = MULTI`.
- SUBCHAIN: never queried, never written (AMBI-07)

---

## 4. Governed Seed File

**Path:** `configs/catalog_seeds/canal_unified_seed.csv`  
**Rows:** 92 (1 header + 91 data rows)

| Source | Rows | Description |
|---|---|---|
| IBP | 3 | UTT, DTT, CAM L1 entries |
| SELL_IN | 3 | UTT, DTT, NA L1 entries |
| SELL_OUT | 86 | All live FORMAT values |

**Schema:**
```
source_system, source_column, canal_raw, canal_raw_norm, canal_level,
gran_canal_grp, canal_type, promoted, business_rule, confirmation_status, notes
```

**Classification summary:**
- **72 FORMAT rows → UTT** (Modern Trade: hypermarkets, supermarkets, clubs, e-commerce, CEDIS)
- **18 FORMAT rows → DTT** (Proximity: all 7E-\* formats, OXXO, CASH AND CARRY)
- CASH AND CARRY → DTT with `CONFIRMED_RECOMMENDED` (business sign-off recommended)

**Design principle:** The notebook reads this seed at runtime. The Python
`GRAN_CANAL_MAP` dict inside the notebook is a **fallback only** — used if
the seed file is unavailable on DBFS.

---

## 5. Notebook Architecture — `build_cat_canal.py`

**Path:** `notebooks/master_catalog/build_cat_canal.py`  
**Lines:** 1,223 | **Version:** 6.0.0

### Section Map

| Section | Tag | What it does |
|---|---|---|
| **S0** | Setup | Constants, logging helpers, `normalize()`, `canal_key()` UDF, Snowflake read-only guard (`_ALLOWED`/`_BLOCKED` regex), credential loader, DBFS init, seed loader |
| **S1** | IBP | Queries IBP 5-level hierarchy, applies V1+V2 gates, builds L1+L2 DataFrames, writes `ibp_canal_hierarchy.csv` |
| **S2** | SELL_IN | Queries `V_D_CLIENT`, builds L1 catalog rows via `L1_MAP`, profiles `lv6_hie_cus_dsc` (V5 threshold gate), applies V3+V4 gates |
| **S3** | SELL_OUT | Loads seed, left-joins live FORMATs, classifies, computes CHAIN `grp_count` in Python post-join (`collect_set`), writes coverage report + FORMAT catalog + CHAIN reference rows, V6 gate |
| **S4** | Volume | Schema discovery (`INFORMATION_SCHEMA.COLUMNS`), IBP value split (always), SELL_IN `VW_FACT_RNV` revenue split (conditional), V10 gate |
| **S5** | Cross-val | Set arithmetic IBP ↔ SELL_IN L1, CAM IBP-only confirmed, V9 gate |
| **S6** | Assemble | `_align()` union of all 6 DataFrames, dedup by 5-column key, V7+V8 DataFrame assertions, V12 BLOCKER, writes 3 output CSVs |
| **S7** | Report | Dynamic f-string summary → DBFS + repo log path |

### Snowflake Read-Only Guard (`run_sf`)

```python
_ALLOWED = re.compile(r'^\s*(SELECT|WITH)\b', re.IGNORECASE)
_BLOCKED = re.compile(
    r'\b(INSERT|UPDATE|DELETE|MERGE|CREATE|DROP|TRUNCATE|ALTER|
         REPLACE|COPY|GRANT|REVOKE|PUT|GET|REMOVE|UNDROP|CLONE)\b',
    re.IGNORECASE
)
def run_sf(sql, db):
    if not _ALLOWED.match(sql):  → blocker(V11)
    if _BLOCKED.search(sql):     → blocker(V11)
    return spark.read.format("net.snowflake.spark.snowflake")...
```

### `canal_key` — Row Deduplication Hash

```python
canal_key = md5(f"{source_system}|{source_column}|{canal_level}|{canal_raw_norm}")[:16]
```

Deduplication key (5-column):
```
(source_system, source_column, canal_level, canal_raw_norm, gran_canal_grp)
```

### `lv6_hie_cus_dsc` Promotion Decision Tree

```
null_rate > 30%   → REFERENCE_ONLY (insufficient data coverage)
distinct < 2      → REFERENCE_ONLY (no segmentation value)
distinct > 50     → REFERENCE_ONLY (grain-level data, not a channel dimension)
else              → promoted = YES
```

Run-3 outcome: `distinct = 23,532 > 50 → REFERENCE_ONLY` ✅ correct.

---

## 6. Output Files

**DBFS Base:** `dbfs:/mnt/mdp/mdm/master_catalog/canal/`

| File | Rows (run-3) | promoted | Description |
|---|---|---|---|
| `cat_canal.csv` | **97** | YES only | Canonical promoted catalog: gran_canal_grp ∈ {UTT, DTT, CAM} |
| `cat_canal_pending.csv` | **0** | PENDING | Unresolved formats (empty = all formats classified ✅) |
| `cat_canal_reference.csv` | 24,727 | YES+NO+PENDING | Full universe: all rows incl. lv6 reference + CHAIN rows |
| `ibp_canal_hierarchy.csv` | written | — | IBP 5-level: GC→CANAL→GRUPO→CADENA→FM |
| `sellout_format_gran_canal.csv` | **86** | — | FORMAT → gran_canal_grp classification per format |
| `sellout_format_seed_coverage.csv` | written | — | LIVE_AND_SEEDED / LIVE_NOT_SEEDED / SEEDED_NOT_LIVE |
| `canal_volume_validation.csv` | **5** | — | IBP (3 rows) + SELL_IN (2 rows) UTT/DTT revenue split |
| `build_cat_canal_report.txt` | 10,471 bytes | — | Full governance log with all gate results |

**Repo log copy:** `logs/catalog_eda/build_cat_canal_report.txt`

---

## 7. Final Schema — 17 Columns

```
canal_key             VARCHAR   -- md5(source_system|source_column|canal_level|canal_raw_norm)[:16]
canal_std             VARCHAR   -- canonical channel name
canal_type            VARCHAR   -- COMMERCIAL_CHANNEL / RETAIL_FORMAT
canal_level           VARCHAR   -- L1_GRAN_CANAL / L2_CANAL / L1_FORMAT / L2_CHAIN / L2_TIPO_CLIENTE
canal_raw             VARCHAR   -- raw value from source (original casing)
canal_raw_norm        VARCHAR   -- TRIM(UPPER(canal_raw)) — join/dedup key
gran_canal_grp        VARCHAR   -- UTT / DTT / CAM / MULTI / UNKNOWN / PENDING
parent_canal_std      VARCHAR   -- parent level value (null for L1)
l2_channel            VARCHAR   -- L2 descriptive value where applicable
source_system         VARCHAR   -- IBP / SELL_IN / SELL_OUT
source_column         VARCHAR   -- exact source column name
promoted              VARCHAR   -- YES / NO / PENDING
confirmation_status   VARCHAR   -- CONFIRMED / CONFIRMED_RECOMMENDED / REFERENCE_ONLY / EXCLUDED / UNKNOWN
row_count             BIGINT    -- evidence count from source query
catalog_date          DATE      -- build run date
catalog_version       VARCHAR   -- "6.0.0"
notes                 VARCHAR   -- business rule / exclusion / pending rationale
```

---

## 8. Volume Validation — `canal_volume_validation.csv` (5 rows)

| Source | GRAN_CANAL | Metric | Note |
|---|---|---|---|
| IBP | UTT | valor_plan (VALOR) | Dynamic from VW_FACT_DANONE_IBP WHERE NOMBRE_ETIQUETA='REAL' |
| IBP | DTT | valor_plan | — |
| IBP | CAM | valor_plan | — |
| SELL_IN | DTT | BIL_INV revenue | VW_FACT_RNV join V_D_CLIENT via SHP_CUS_IDT + SAL_ORG_COD |
| SELL_IN | UTT | BIL_INV revenue | BIL_DAT >= 20250101 |

**Confirmed SELL_IN fact table columns** (OQ1 resolved 2026-06-29):
- Join: `FAC.SHP_CUS_IDT = CLI.CUS_IDT AND FAC.SAL_ORG_COD = CLI.SAL_ORG_COD`
- Date: `BIL_DAT` (YYYYMMDD integer, filter `>= 20250101`)
- Value: `BIL_INV` (billed invoice = net revenue)
- Volume: `LITER` (WATERS) / `BIL_NET_KGR` (EDP) — not used in canal split

---

## 9. Validation Gate Summary (V1–V12)

| Gate | Type | Condition | Run-3 |
|---|---|---|---|
| **V1** IBP UTT+DTT | BLOCKER | Both present in GRAN_CANAL | ✅ PASS |
| **V2** CAM not remapped | ASSERT | gran_canal_grp=CAM — never forced to UTT/DTT | ✅ PASS |
| **V3** SELL_IN DTT+UTT | BLOCKER | Both present in cus_grn_chl_dsc | ✅ PASS |
| **V4** Unexpected L1 | WARN | Values outside {DTT, UTT, NA, null} | ✅ PASS |
| **V5** lv6 promotable | WARN | null ≤30%, distinct ∈ [2, 50] | ⚠️ WARN (23,532 > 50, REFERENCE_ONLY) |
| **V6** FORMAT seed | WARN | All live FORMATs in seed | ✅ PASS (86/86) |
| **V7** No PENDING in csv | ASSERT | promoted≠PENDING in cat_canal.csv | ✅ PASS |
| **V8** SUBCHAIN absent | ASSERT | source_column/canal_level ≠ SUBCHAIN | ✅ PASS |
| **V9** IBP↔SELL_IN | WARN | Overlap ≥ 60% | ✅ PASS (66.7%) |
| **V10** Volume validation | WARN | canal_volume_validation.csv generated | ✅ PASS |
| **V11** Snowflake RO | BLOCKER | No DML/DDL in any SQL | ✅ PASS |
| **V12** ≥5 promoted rows | BLOCKER | IBP UTT/DTT + SI UTT/DTT + 1 FORMAT | ✅ PASS (97 rows) |

**V5 WARN is expected and correct.** `lv6_hie_cus_dsc` with 23,532 distinct values
is individual customer codes (operational grain), not a channel sub-type. Rows are
preserved in `cat_canal_reference.csv` for lineage/audit.

---

## 10. Build Iteration Log

| Run | Date | Result | Key Issue Fixed |
|---|---|---|---|
| **Run 0** | 2026-06-29 | ❌ FAIL | `IllegalArgumentException`: wrong Databricks secret scope `"mdp"` |
| **Run 1** | 2026-06-29 | ⚠️ 13 WARN | `cat_canal.csv` had 24,183 rows (lv6 grain-level promoted); empty-string L1 WARN |
| **Run 2** | 2026-06-29 | ⚠️ 7 WARN | CALIMAX seed typo; V10 fact table NOT_FOUND |
| **Run 3** | 2026-06-29 | ✅ **PASS** | All issues resolved. 3 expected WARNs remain (non-actionable) |

### Fix Log

| Commit | Fix |
|---|---|
| `39fe437` | Replaced broken Snowflake creds (`"mdp"` scope) with `importlib` pattern + KV scope `DAN-AM-P-KVT800-R-MDP-DB` |
| `b6ae9bf` | `lv6_hie_cus_dsc distinct > 50 → REFERENCE_ONLY`; empty-string L1 filter; V5 comments |
| `d5ea45e` | CALMAX → CALIMAX in seed; V5 gate label shows real reason (distinct, not null_rate) |
| `b4e4ff4` | Hardcoded `VW_FACT_RNV` columns (`SHP_CUS_IDT`, `BIL_DAT`, `BIL_INV`); `BIL_DAT >= 20250101` integer filter |

---

## 11. Key Design Decisions & Rationale

| Decision | Rationale |
|---|---|
| **Seed-file-driven** classification | Keeps business rules out of Python code; business team can update CSV without touching notebook |
| **`CONFIRMED_RECOMMENDED` = promoted** | Avoids over-blocking: CASH AND CARRY's DTT classification is well-reasoned and validated. Status is informational, not a gate. |
| **CHAIN = REFERENCE_ONLY** | CHAIN is not a canonical join key across all sources. Chains spanning multiple gran_canal_grp → `MULTI` in reference. |
| **lv6 `distinct > 50` threshold** | Channel sub-types are coarse categories (expected 2–20). 23,532 distinct values = client IDs, not channel types. |
| **`BIL_DAT` as integer** | Snowflake stores billing date as `YYYYMMDD` integer — string comparison `>= '202501'` would silently fail or scan wrong rows. |
| **`SAL_ORG_COD` in join** | Required per SELL_IN_DICT.txt: `FAC.SHP_CUS_IDT = CLI.CUS_IDT AND FAC.SAL_ORG_COD = CLI.SAL_ORG_COD` |
| **No `stat_cod` filter** | Field is unreliable (AMBI_02). Full `V_D_CLIENT` extracted regardless of status. |
| **SUBCHAIN excluded** | 33.33% null rate (AMBI-07). Never queried, never written to any output file. |
| **`GRAN_CANAL_MAP` Python dict = fallback only** | Belt-and-suspenders: if DBFS seed unavailable, notebook still runs with Python classification |

---

## 12. Files in Repository

```
configs/catalog_seeds/
  └── canal_unified_seed.csv         ← 92 rows — governed FORMAT → gran_canal_grp mapping

notebooks/master_catalog/
  └── build_cat_canal.py             ← 1,223 lines — production notebook (v6.0.0)

logs/catalog_eda/
  └── build_cat_canal_report.txt     ← repo copy of run-3 governance log
```

---

## 13. How to Re-Run

1. **Pull** branch `feature/master-data-catalog-v6` in Databricks
2. **Run all cells** in `notebooks/master_catalog/build_cat_canal.py`
3. **Expected outcome:** 0 BLOCKERS, ≤5 WARNs (V5 always WARN — expected)
4. **Outputs land at:** `dbfs:/mnt/mdp/mdm/master_catalog/canal/`
5. **To update FORMAT classification:** edit `configs/catalog_seeds/canal_unified_seed.csv`
   and re-run — no notebook code changes needed

---

*Documentation generated: 2026-06-29 | Catalog version: 6.0.0 | Branch: feature/master-data-catalog-v6*
