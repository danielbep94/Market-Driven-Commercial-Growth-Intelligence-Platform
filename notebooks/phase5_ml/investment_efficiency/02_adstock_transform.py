# Databricks notebook source
# Phase 5 — MMM: Adstock Transformation
# Transforms raw investment spend into effective investment using adstock decay.
# This notebook runs before 03_mmm_regression.py and produces FACT_INVESTMENT_ADSTOCK.
#
# Target variable: incremental_sell_out_units
# Counterfactual method: adstock decay model
# Formula: adstock_t = spend_t + decay_rate × adstock_(t-1)
# decay_rate estimated per brand × channel in this notebook.

# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from scipy.optimize import minimize_scalar
import numpy as np

ENVIRONMENT = "dev"
SNOWFLAKE_DB = f"MGI_{ENVIRONMENT.upper()}"

# COMMAND ----------
# MAGIC %md ## 1. Load Investment Data (Gold Layer — from Snowflake)

# COMMAND ----------
df_investment = spark.sql(f"""
    SELECT
        brand_key,
        channel_key,
        week_key,
        total_spend,
        brand_name,
        channel_name
    FROM {SNOWFLAKE_DB}.GOLD.FACT_INVESTMENT fi
    JOIN {SNOWFLAKE_DB}.GOLD.DIM_BRAND db ON fi.brand_key = db.brand_key
    JOIN {SNOWFLAKE_DB}.GOLD.DIM_CHANNEL dc ON fi.channel_key = dc.channel_key
    WHERE db.is_current = TRUE
    ORDER BY brand_key, channel_key, week_key
""")

# COMMAND ----------
# MAGIC %md ## 2. Validate Investment Range (Edge Case: sparse variation)
#
# If a brand × channel combination shows < 20% coefficient of variation in spend,
# the adstock curve cannot be reliably estimated.
# Flag these as saturation_not_observable = TRUE.

# COMMAND ----------
investment_cv = df_investment.groupBy("brand_key", "channel_key").agg(
    F.stddev("total_spend").alias("spend_stddev"),
    F.mean("total_spend").alias("spend_mean"),
    (F.stddev("total_spend") / F.mean("total_spend")).alias("coeff_variation")
)

sparse_investment = investment_cv.filter(F.col("coeff_variation") < 0.20)
print(f"Brand × channel combinations with sparse investment variation: {sparse_investment.count()}")
print("These will be flagged saturation_not_observable = TRUE in model outputs.")

# COMMAND ----------
# MAGIC %md ## 3. Estimate Adstock Decay Rate Per Brand × Channel
#
# For each brand × channel, estimate the decay_rate that maximizes
# the correlation between adstock-transformed spend and sell-out.
# Search range: [0.1, 0.9] per model_config.yaml

# COMMAND ----------
# TODO: Implement decay rate estimation per brand × channel
# 1. For each candidate decay_rate in search range:
#    a. Compute adstock series
#    b. Compute Pearson correlation with sell_out_units
# 2. Select decay_rate with highest correlation
# 3. Store in MODEL_METADATA
print("Adstock decay rate estimation: TO BE IMPLEMENTED in Phase 5")
print("Estimated decay_rate values must be stored in MODEL_METADATA before 03_mmm_regression.py runs")

# COMMAND ----------
# MAGIC %md ## 4. Apply Adstock Transformation

# COMMAND ----------
# TODO: Apply estimated decay_rate per brand × channel to compute adstock series
# Use PySpark window function for time-ordered recursive computation
# Write to: FACT_INVESTMENT_ADSTOCK (temporary table for MMM input)
print("Adstock transformation: TO BE IMPLEMENTED with estimated decay rates from step 3")
