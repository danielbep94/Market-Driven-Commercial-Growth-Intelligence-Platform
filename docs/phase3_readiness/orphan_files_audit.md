# Repository Hygiene Report — Orphan Files Audit

**Date:** 2026-06-27  
**Branch:** MDM  
**Commit:** 30e38d3

---

## 🚨 IMMEDIATE ACTION REQUIRED

### SECURITY — `validate_semantic_layer_phase_b.py` (CRITICAL)

**File:** `notebooks/validate_semantic_layer_phase_b.py`  
**Issue:** Lines 28–32 contain a **hardcoded plaintext Snowflake password**:
```python
sf_opts_mex = {
    "sfUser":     "PRD_OSM_DPH_READER",
    "sfPassword": "73.bBZmne7Aq",   # EXPOSED PLAINTEXT CREDENTIAL
    ...
}
```
**Action required (before any other cleanup):**
1. DBA must rotate the `PRD_OSM_DPH_READER` Snowflake password immediately
2. Update Key Vault scope `DAN-AM-P-KVT800-R-MEX-DB` with new password
3. If this repo is on GitHub, purge the credential from git history using `git filter-branch` or BFG Repo Cleaner
4. Delete this file from the repo

**Reference:** `configs/tech_debt.yaml` → TD-006 (HIGH severity, deadline 2026-07-01)

---

## Orphan Files Classification

### notebooks/ root-level files

| File | Size | Last Modified | Classification | Recommendation |
|---|---|---|---|---|
| `validate_credentials.py` | 7.6 KB | Jun 22 | ✅ CURRENT — shared prerequisite utility | **KEEP** |
| `validate_mdm_catalogs.py` | 30 KB | Jun 25 | ✅ CURRENT — Notebook A (v3), upstream of Phase 3 | **KEEP** |
| `validate_mdm_cross_source_joins.py` | 28 KB | Jun 25 | ✅ CURRENT — Notebook B (v3), join rules proof | **KEEP** |
| `phase2_mdm_signoff.py` | 51 KB | Jun 26 | ✅ PHASE 2 HISTORY — foundational signoff document | **KEEP (archive)** |
| `phase3_mdm_validation.py` | 14 KB | Jun 26 | ✅ CURRENT — main Phase 3 gate notebook | **KEEP** |
| `extract_column_types.py` | 14 KB | Jun 22 | ⚠️ UTILITY — column type extraction script from Phase 1 | **KEEP** — referenced by `column_types_snapshot.yaml` |
| `validate_cross_source_joins_phase_d.py` | 46 KB | Jun 25 | ⚠️ LEGACY — Phase D, superseded by Phase 3 A1–A6 | **ARCHIVE** to `docs/phase_outputs/` |
| `validate_nielsen_marca_std_v12.py` | 26 KB | Jun 22 | ⚠️ LEGACY — Phase C.9, MARCA_STD injection done | **ARCHIVE** to `docs/phase_outputs/` |
| `validate_semantic_layer_phase_b.py` | 14 KB | Jun 25 | 🚨 LEGACY + SECURITY — Phase B, hardcoded password | **DELETE** (rotate credentials first) |

### notebooks/ root-level text files (validation outputs)

| File | Size | Phase | Recommendation |
|---|---|---|---|
| `validation_results.txt` | 32 KB | Phase B | **ARCHIVE** to `docs/phase_outputs/` |
| `validation_results_nielsen_v12.txt` | 35 KB | Phase C.9 | **ARCHIVE** to `docs/phase_outputs/` |
| `validation_results_phase_c.txt` | 97 KB | Phase B/C (misleading name) | **ARCHIVE + RENAME** to `validation_results_phase_b_rerun.txt` |
| `validation_results_phase_d.txt` | 13 KB | Phase D | **ARCHIVE** to `docs/phase_outputs/` |

### notebooks/phase3_silver/ — stub notebooks

All 6 are empty stubs (1 line: `# Databricks notebook source`):

| File | Status | Recommendation |
|---|---|---|
| `silver_forecast.py` | ⛔ STUB | **KEEP** — Phase 3+ placeholder, document in README |
| `silver_inventory.py` | ⛔ STUB | **KEEP** — Phase 3+ placeholder |
| `silver_price.py` | ⛔ STUB | **KEEP** — Phase 3+ placeholder |
| `silver_promotions.py` | ⛔ STUB | **KEEP** — Phase 3+ placeholder |
| `silver_waste.py` | ⛔ STUB | **KEEP** — Phase 3+ placeholder |
| `silver_investment.py` | ⛔ STUB | **KEEP** — Phase 3+ placeholder |

> These are future-phase placeholders. Add a comment to each explaining they are stubs for Phase 4+.

### scripts/ — helper scripts

| File | Status | Recommendation |
|---|---|---|
| `m1_nielsen_classify.py` | ✅ ACTIVE | **KEEP** — M1 classification, documented |
| `build_registry.py` | UNKNOWN | **REVIEW** — not referenced by Phase 3; may be Phase 1 |
| `extract_column_types.py` | PHASE 1 UTILITY | **KEEP** — produced `column_types_snapshot.yaml` |
| `generate_marca_std.py` | PHASE 1 UTILITY | **KEEP** — produced `brand_crosswalk.yaml` |
| `homologize_volume.py` | UNKNOWN | **REVIEW** — no reference found in Phase 3 |
| `patch_marca_std.py` | PHASE C UTILITY | **ARCHIVE** — Phase C.5 one-off patch |
| `schema_validator.py` | UTILITY | **KEEP** — general schema validation |
| `setup_databricks_secrets.sh` | SETUP | **KEEP** — onboarding script |
| `setup_databricks_secrets_windows.ps1` | SETUP | **KEEP** — onboarding script |
| `setup_snowflake_schemas.sh` | SETUP | **KEEP** — onboarding script |
| `validate_homologation_dict.py` | UTILITY | **KEEP** — validates homologation dictionary |
| `__pycache__/` | CACHE | **DELETE** from git-tracked files (add to .gitignore) |

### powerbi/ — Power BI files

| File | Status | Recommendation |
|---|---|---|
| `Market_Growth_Intelligence.pbix.placeholder` | PLACEHOLDER | **KEEP** — placeholder for the actual PBIX |
| `docs/page_specs/` | CURRENT | **KEEP** — page specifications for Phase 5 |
| `themes/mgi_theme.json` | CURRENT | **KEEP** — Power BI theme |

---

## Summary Action Table

| Priority | Action | Files Affected |
|---|---|---|
| 🚨 IMMEDIATE | Rotate Snowflake password, then delete file | `notebooks/validate_semantic_layer_phase_b.py` |
| 🔴 HIGH | Add to `.gitignore` | `scripts/__pycache__/`, `configs/snowflake_creds.py` |
| 🟡 MEDIUM | Archive to `docs/phase_outputs/` | 4 validation result `.txt` files, 2 legacy notebooks |
| 🟡 MEDIUM | Add stub comment to files | 6 empty `silver_*.py` stubs |
| 🟢 LOW | Review and document or archive | `scripts/homologize_volume.py`, `scripts/patch_marca_std.py` |

---

## Recommended .gitignore Additions

```gitignore
# Python cache
__pycache__/
*.pyc
*.pyo

# Snowflake credentials (NEVER commit)
configs/snowflake_creds.py

# Large log outputs (optional — if tracked, add to LFS instead)
# logs/nielsen_facts_std.csv
```
