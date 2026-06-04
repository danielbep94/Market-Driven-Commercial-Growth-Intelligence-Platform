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

# Phase 1 — Data Profiling: NIELSEN_MARKET
# Pattern: same as 01_data_profiling_sell_in.py
# Run on WORK COMPUTER in Databricks.
# Output: docs/phase_outputs/phase1_data_inventory.md (append this source's section)

# COMMAND ----------
ENVIRONMENT  = "dev"
SOURCE_NAME  = "NIELSEN_MARKET"
SOURCE_TABLE = f"MGI_{'{'}ENVIRONMENT.upper(){'}'}.BRONZE.NIELSEN_MARKET_RAW"
DATE_COL     = "period_end_date"
UK_COLS      = "brand_id,category_id,period_end_date".split(",")

# COMMAND ----------
# Follow the exact same profiling steps as 01_data_profiling_sell_in.py:
# 1. Load table        → row count, column count
# 2. Schema snapshot   → column names, types, nullable
# 3. Null rates        → flag any column > 1% or > 5%
# 4. Date range        → min, max, distinct weeks
# 5. Cardinality       → distinct SKUs, customers, etc.
# 6. Numeric ranges    → units, revenue, waste_units, etc.
# 7. Duplicate check   → on UK_COLS
# 8. Sample rows       → display(df.limit(5))
# 9. Write output to docs/phase_outputs/phase1_data_inventory.md (append NIELSEN_MARKET section)
# 10. Print git commit instructions

print("Implement profiling steps following 01_data_profiling_sell_in.py pattern.")
print(f"Source: {SOURCE_NAME} | Table: {SOURCE_TABLE}")
