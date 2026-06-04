# Databricks notebook source

# ── Azure Key Vault Connection ────────────────────────────────────────────────
KEYVAULT_NAME = "DAN-AM-P-KVT800-R-MDP-DB"
KEY_NAME_USR  = "snowflake-user"
KEY_NAME_PWD  = "snowflake-password"
SF_URL        = "danonenam.east-us-2.azure.snowflakecomputing.com"
SF_WAREHOUSE  = "PRD_MDP_ANL_WH"

try:
    user     = dbutils.secrets.get(scope=KEYVAULT_NAME, key=KEY_NAME_USR)
    password = dbutils.secrets.get(scope=KEYVAULT_NAME, key=KEY_NAME_PWD)
    print(f"✅ Credentials loaded from: {KEYVAULT_NAME}")
except NameError:
    print("\n🚨 WARNING: Non-Databricks env. Using MOCK credentials.")
    user = "MOCK_USER"
    password = "MOCK_PASSWORD"
except Exception as e:
    print(f"\n🚨 WARNING: Could not retrieve secrets: {e}. Using MOCK credentials.")
    user = "MOCK_USER"
    password = "MOCK_PASSWORD"

def get_sf_options(db_name, schema_name="PUBLIC"):
    return {
        "sfURL": SF_URL, "sfUser": user, "sfPassword": password,
        "sfDatabase": db_name, "sfSchema": schema_name, "sfWarehouse": SF_WAREHOUSE,
    }

# Phase 1 — Data Readiness Scorecard
# Run LAST in the Phase 1 sequence, after all 10 sources are profiled.
# Aggregates findings into a 0–100 score per source.
# Output: docs/phase_outputs/phase1_data_readiness_scorecard.md
# This document is the formal Phase 1 completion artifact.

# COMMAND ----------
from datetime import datetime
RUN_AT = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
OUTPUT_FILE = "docs/phase_outputs/phase1_data_readiness_scorecard.md"

# ── Fill in scores from the individual profiling notebooks ─────────────────
# Score each source 0–100 on four dimensions (25 pts each):
# 1. Completeness (null rates on key fields)
# 2. Temporal coverage (date range, no major gaps)
# 3. Volume sanity (plausible values, no impossible negatives/extremes)
# 4. Join key coverage (% matched to other sources)

# UPDATE THESE after reviewing all profiling outputs:
source_scores = {
    "SELL_IN":     {"completeness": 0, "temporal": 0, "volume": 0, "join_keys": 0, "notes": "TBD"},
    "SELL_OUT":    {"completeness": 0, "temporal": 0, "volume": 0, "join_keys": 0, "notes": "TBD"},
    "WASTE":       {"completeness": 0, "temporal": 0, "volume": 0, "join_keys": 0, "notes": "TBD"},
    "INVESTMENT":  {"completeness": 0, "temporal": 0, "volume": 0, "join_keys": 0, "notes": "TBD"},
    "FORECAST":    {"completeness": 0, "temporal": 0, "volume": 0, "join_keys": 0, "notes": "TBD"},
    "NIELSEN":     {"completeness": 0, "temporal": 0, "volume": 0, "join_keys": 0, "notes": "TBD"},
    "PRICE":       {"completeness": 0, "temporal": 0, "volume": 0, "join_keys": 0, "notes": "TBD"},
    "PROMOTIONS":  {"completeness": 0, "temporal": 0, "volume": 0, "join_keys": 0, "notes": "TBD"},
    "INVENTORY":   {"completeness": 0, "temporal": 0, "volume": 0, "join_keys": 0, "notes": "TBD"},
    "CALENDAR":    {"completeness": 0, "temporal": 0, "volume": 0, "join_keys": 0, "notes": "TBD"},
}

# COMMAND ----------
def total_score(s):
    return s["completeness"] + s["temporal"] + s["volume"] + s["join_keys"]

def readiness_label(score):
    if score >= 80: return "🟢 READY"
    if score >= 60: return "🟡 CONDITIONAL"
    return "🔴 NOT READY"

rows = "\n".join([
    f"| {src} | {s['completeness']} | {s['temporal']} | {s['volume']} | {s['join_keys']} | **{total_score(s)}** | {readiness_label(total_score(s))} | {s['notes']} |"
    for src, s in source_scores.items()
])

any_not_ready = any(total_score(s) < 60 for s in source_scores.values())
phase2_gate   = "❌ BLOCKED — resolve NOT READY sources first" if any_not_ready else "✅ CLEARED"

output = f"""# Phase 1 — Data Readiness Scorecard

**Generated:** {RUN_AT}
**Phase 2 Gate:** {phase2_gate}

Scoring: each dimension = 0–25 pts | Total = 0–100 pts
- 🟢 READY (≥80): proceed without conditions
- 🟡 CONDITIONAL (60–79): proceed with documented mitigation
- 🔴 NOT READY (<60): must resolve before Phase 2

---

| Source | Completeness /25 | Temporal /25 | Volume /25 | Join Keys /25 | **Total /100** | Status | Notes |
|--------|-----------------|-------------|-----------|--------------|---------------|--------|-------|
{rows}

---

## Phase 2 Gate Decision

**Status: {phase2_gate}**

## Sign-Off

| Name | Role | Signature | Date |
|------|------|-----------|------|
| | Data Engineer | | |
| | Data Steward | | |
| | PM | | |
"""

with open(OUTPUT_FILE, "w") as f:
    f.write(output)

print(f"✅ Scorecard written to {OUTPUT_FILE}")
print(f"Phase 2 gate: {phase2_gate}")
print("\nCommit and push — then tell the agent: 'Phase 1 complete, scorecard committed'")
