# Databricks notebook source
# Phase 1 — Join Key Validation
# Validates that join keys match across all data source pairs.
# This notebook must be run and reviewed before any Phase 2 work begins.
# Output: JOIN_KEY_VALIDATION_REPORT table in MONITORING schema

# COMMAND ----------
# MAGIC %md
# MAGIC # Join Key Validation Report
# MAGIC
# MAGIC **Objective:** Validate that join keys (SKU codes, customer codes, brand codes, date keys)
# MAGIC match across all source pairs. Mismatched join keys are the single most common cause of model failure.
# MAGIC
# MAGIC **Required before:** Phase 2 data model design
# MAGIC **Output:** `MONITORING.JOIN_KEY_VALIDATION_REPORT`

# COMMAND ----------
import pyspark.sql.functions as F
from datetime import datetime

# Configuration — set environment before running
ENVIRONMENT = "dev"  # dev | staging | prod
RUN_TIMESTAMP = datetime.utcnow().isoformat()

# COMMAND ----------
# MAGIC %md ## 1. Load Source Data (Bronze Layer)

# COMMAND ----------
# Load all Bronze tables
df_sell_in = spark.table(f"mgi_{ENVIRONMENT}.bronze.sell_in")
df_sell_out = spark.table(f"mgi_{ENVIRONMENT}.bronze.sell_out")
df_waste = spark.table(f"mgi_{ENVIRONMENT}.bronze.waste")
df_investment = spark.table(f"mgi_{ENVIRONMENT}.bronze.investment")
df_forecast = spark.table(f"mgi_{ENVIRONMENT}.bronze.forecast")
df_nielsen = spark.table(f"mgi_{ENVIRONMENT}.bronze.nielsen")

# COMMAND ----------
# MAGIC %md ## 2. SKU Key Validation

# COMMAND ----------
# Get distinct SKU IDs from each source
sell_in_skus = df_sell_in.select(F.col("sku_id").alias("sku")).distinct()
sell_out_skus = df_sell_out.select(F.col("sku_id").alias("sku")).distinct()
waste_skus = df_waste.select(F.col("sku_id").alias("sku")).distinct()
forecast_skus = df_forecast.select(F.col("sku_id").alias("sku")).distinct()

# SKUs in sell-out NOT in sell-in (orphan sell-out records)
orphan_sell_out_skus = sell_out_skus.subtract(sell_in_skus)
print(f"SKUs in sell-out NOT in sell-in: {orphan_sell_out_skus.count()}")

# SKUs in waste NOT in sell-in
orphan_waste_skus = waste_skus.subtract(sell_in_skus)
print(f"SKUs in waste NOT in sell-in: {orphan_waste_skus.count()}")

# SKUs in forecast NOT in sell-in
orphan_forecast_skus = forecast_skus.subtract(sell_in_skus)
print(f"SKUs in forecast NOT in sell-in: {orphan_forecast_skus.count()}")

# COMMAND ----------
# MAGIC %md ## 3. Customer Key Validation

# COMMAND ----------
sell_in_customers = df_sell_in.select(F.col("customer_id").alias("customer")).distinct()
sell_out_customers = df_sell_out.select(F.col("customer_id").alias("customer")).distinct()
waste_customers = df_waste.select(F.col("customer_id").alias("customer")).distinct()

orphan_sell_out_customers = sell_out_customers.subtract(sell_in_customers)
print(f"Customers in sell-out NOT in sell-in: {orphan_sell_out_customers.count()}")

orphan_waste_customers = waste_customers.subtract(sell_in_customers)
print(f"Customers in waste NOT in sell-in: {orphan_waste_customers.count()}")

# COMMAND ----------
# MAGIC %md ## 4. Brand Key Validation (across sources and Nielsen)

# COMMAND ----------
# Nielsen brand names vs. sell-in brand names
# This will almost certainly reveal naming inconsistencies
sell_in_brands = df_sell_in.select(F.col("brand_name").alias("brand")).distinct() if "brand_name" in df_sell_in.columns else None
nielsen_brands = df_nielsen.select(F.col("brand_name").alias("brand")).distinct() if "brand_name" in df_nielsen.columns else None

if sell_in_brands and nielsen_brands:
    nielsen_not_in_sell_in = nielsen_brands.subtract(sell_in_brands)
    print(f"Brand names in Nielsen NOT matching sell-in: {nielsen_not_in_sell_in.count()}")
    if nielsen_not_in_sell_in.count() > 0:
        print("Sample mismatches:")
        nielsen_not_in_sell_in.show(20, truncate=False)

# COMMAND ----------
# MAGIC %md ## 5. Date Key Validation

# COMMAND ----------
# Validate that date ranges overlap meaningfully across sources
date_ranges = {}
for name, df, date_col in [
    ("sell_in", df_sell_in, "ship_date"),
    ("sell_out", df_sell_out, "sell_out_date"),
    ("waste", df_waste, "waste_date"),
    ("forecast", df_forecast, "forecast_date"),
]:
    if date_col in df.columns:
        agg = df.agg(F.min(date_col).alias("min_date"), F.max(date_col).alias("max_date")).collect()[0]
        date_ranges[name] = {"min": agg["min_date"], "max": agg["max_date"]}
        print(f"{name}: {agg['min_date']} → {agg['max_date']}")

# COMMAND ----------
# MAGIC %md ## 6. Write Validation Report

# COMMAND ----------
# TODO: Collect all validation results into a structured report
# and write to MONITORING.JOIN_KEY_VALIDATION_REPORT
# This should include: source_pair, key_type, match_rate, orphan_count, run_timestamp
print("Join key validation complete. Review results above.")
print("ACTION REQUIRED: If orphan count > 0, resolve before Phase 2 begins.")
print("PHASE 2 BLOCKER: Brand name mismatches must be added to homologation dictionary.")
