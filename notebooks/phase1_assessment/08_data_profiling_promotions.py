# Databricks notebook source
# Phase 1 — Data Profiling: PROMOTIONS
# Pattern: same as 01_data_profiling_sell_in.py
# Run on WORK COMPUTER in Databricks.
# Output: docs/phase_outputs/phase1_data_inventory.md (append this source's section)

# COMMAND ----------
ENVIRONMENT  = "dev"
SOURCE_NAME  = "PROMOTIONS"
SOURCE_TABLE = f"MGI_{'{'}ENVIRONMENT.upper(){'}'}.BRONZE.PROMOTIONS_RAW"
DATE_COL     = "promo_start_date"
UK_COLS      = "brand_id,customer_id,promo_start_date".split(",")

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
# 9. Write output to docs/phase_outputs/phase1_data_inventory.md (append PROMOTIONS section)
# 10. Print git commit instructions

print("Implement profiling steps following 01_data_profiling_sell_in.py pattern.")
print(f"Source: {SOURCE_NAME} | Table: {SOURCE_TABLE}")
