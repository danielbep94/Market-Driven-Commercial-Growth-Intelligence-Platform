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

# Phase 1 — Join Key Validation (PHASE 2 GATE)
# Run on WORK COMPUTER in Databricks.
# Output: docs/phase_outputs/phase1_join_key_validation.md
#
# CRITICAL: If join key mismatch > 20% on any source pair, Phase 2 cannot begin.
# This notebook produces the signed-off gate document.

# COMMAND ----------
ENVIRONMENT = "dev"
DB = f"MGI_{'{'}ENVIRONMENT.upper(){'}'}"
OUTPUT_FILE = "docs/phase_outputs/phase1_join_key_validation.md"

from datetime import datetime
RUN_AT = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

# COMMAND ----------
# MAGIC %md ## Join Pairs to Validate
# MAGIC Each pair checks whether keys in the fact table exist in the dimension or other fact.

# COMMAND ----------
results = []

def check_join(left_table, right_table, left_key, right_key, pair_label):
    """Check what % of left_key values exist in right_key."""
    left  = spark.table(f"{DB}.BRONZE.{left_table}").select(left_key).distinct()
    right = spark.table(f"{DB}.BRONZE.{right_table}").select(right_key).distinct()
    left_count    = left.count()
    matched_count = left.join(right, left[left_key] == right[right_key], "inner").count()
    match_pct     = round(matched_count / left_count * 100, 1) if left_count > 0 else 0
    unmatched_pct = 100 - match_pct
    status = "✅ PASS" if unmatched_pct <= 20 else "❌ FAIL — PHASE 2 BLOCKED"
    result = {
        "pair": pair_label,
        "left": f"{left_table}.{left_key}",
        "right": f"{right_table}.{right_key}",
        "left_count": left_count,
        "matched": matched_count,
        "match_pct": match_pct,
        "unmatched_pct": unmatched_pct,
        "status": status
    }
    results.append(result)
    print(f"{status}  {pair_label}: {match_pct:.1f}% match ({unmatched_pct:.1f}% unmatched)")
    return result

# COMMAND ----------
# MAGIC %md ## Run Join Key Checks
# MAGIC Adjust table/column names to match your actual Snowflake Bronze schema.

# COMMAND ----------
# Sell-Out SKUs in Sell-In (most critical — shared SKU dimension)
check_join("SELL_OUT_RAW", "SELL_IN_RAW", "sku_id", "sku_id",
           "Sell-Out SKU → Sell-In SKU")

# Sell-Out customers in customer master
check_join("SELL_OUT_RAW", "SELL_IN_RAW", "customer_id", "customer_id",
           "Sell-Out Customer → Sell-In Customer")

# Waste SKUs in Sell-In
check_join("WASTE_RAW", "SELL_IN_RAW", "sku_id", "sku_id",
           "Waste SKU → Sell-In SKU")

# Forecast SKUs in Sell-In
check_join("FORECAST_RAW", "SELL_IN_RAW", "sku_id", "sku_id",
           "Forecast SKU → Sell-In SKU")

# Investment brands in Nielsen brands
check_join("INVESTMENT_RAW", "NIELSEN_MARKET_RAW", "brand_id", "brand_id",
           "Investment Brand → Nielsen Brand")

# Promotions in Sell-In customers
check_join("PROMOTIONS_RAW", "SELL_IN_RAW", "customer_id", "customer_id",
           "Promotions Customer → Sell-In Customer")

# Inventory SKUs in Sell-In
check_join("INVENTORY_RAW", "SELL_IN_RAW", "sku_id", "sku_id",
           "Inventory SKU → Sell-In SKU")

# COMMAND ----------
# MAGIC %md ## Write Output

# COMMAND ----------
rows = "\n".join([
    f"| {r['pair']} | {r['left']} | {r['right']} | {r['left_count']:,} | {r['match_pct']}% | {r['unmatched_pct']}% | {r['status']} |"
    for r in results
])

blocked = [r for r in results if "FAIL" in r["status"]]
gate_status = "❌ BLOCKED" if blocked else "✅ CLEARED"

output = f"""# Phase 1 — Join Key Validation Report

**Generated:** {RUN_AT}
**Environment:** {ENVIRONMENT.upper()}
**Phase 2 Gate Status:** {gate_status}

---

## Results

| Source Pair | Left Key | Right Key | Left Count | Match % | Unmatched % | Status |
|-------------|----------|-----------|-----------|---------|-------------|--------|
{rows}

## Gate Decision

**Threshold:** Unmatched % must be ≤ 20% for all pairs to pass.

{"### ❌ BLOCKED — Resolution required before Phase 2" + chr(10) + chr(10) + chr(10).join([f"- **{r['pair']}**: {r['unmatched_pct']}% unmatched — investigate missing keys" for r in blocked]) if blocked else "### ✅ CLEARED — Phase 2 can begin"}

## Open Items (fill in after reviewing)

- [ ] For any FAIL: are the unmatched keys new records, test data, or a real mapping gap?
- [ ] Are there homologation issues (same entity, different ID format across sources)?
- [ ] Document resolution approach for each FAIL before starting Phase 2

## Sign-Off

| Name | Role | Decision | Date |
|------|------|----------|------|
| | Data Engineer | | |
| | Data Steward | | |
"""

with open(OUTPUT_FILE, "w") as f:
    f.write(output)

print(f"\n✅ Output written to {OUTPUT_FILE}")
print(f"Phase 2 gate: {gate_status}")
print(f"\nNext steps on work computer:")
print(f"  git add {OUTPUT_FILE}")
print(f"  git commit -m 'data: phase1 join key validation — gate {gate_status}'")
print(f"  git push origin main")
