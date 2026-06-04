# Databricks notebook source
# Phase 5 — Competitive Risk Score v1
# Rule-based weighted composite of 5 Nielsen signals.
# NO clustering in v1 — clustering deferred to v2.
# See: docs/ml_model_cards/model_card_competitive_risk.md
#
# Output: competitive_risk_score and competitive_risk_alert_flag → MART table

# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.window import Window

ENVIRONMENT = "dev"
SNOWFLAKE_DB = f"MGI_{ENVIRONMENT.upper()}"

# Weights from model_config.yaml
WEIGHTS = {
    "share_loss_rate": 0.30,
    "price_gap_widening": 0.25,
    "numeric_dist_change": 0.20,
    "weighted_dist_change": 0.15,
    "share_yoy": 0.10,
}
ALERT_THRESHOLD = 70
ALERT_CONSECUTIVE_PERIODS = 2

# COMMAND ----------
# MAGIC %md ## 1. Load Nielsen Data (Gold Layer — from Snowflake)

# COMMAND ----------
df_nielsen = spark.sql(f"""
    WITH nielsen_with_lag AS (
        SELECT
            brand_key,
            channel_key,
            period_key,
            value_share,
            volume_share,
            numeric_distribution,
            weighted_distribution,
            price_index_vs_category,
            LAG(value_share, 13) OVER (PARTITION BY brand_key, channel_key ORDER BY period_key) AS share_lag_13p,
            LAG(value_share, 52) OVER (PARTITION BY brand_key, channel_key ORDER BY period_key) AS share_lag_52p,
            LAG(numeric_distribution, 13) OVER (PARTITION BY brand_key, channel_key ORDER BY period_key) AS num_dist_lag_13p,
            LAG(weighted_distribution, 13) OVER (PARTITION BY brand_key, channel_key ORDER BY period_key) AS wgt_dist_lag_13p,
            LAG(price_index_vs_category, 13) OVER (PARTITION BY brand_key, channel_key ORDER BY period_key) AS price_idx_lag_13p
        FROM {SNOWFLAKE_DB}.GOLD.FACT_NIELSEN_MARKET
        WHERE brand_key IS NOT NULL
    )
    SELECT *,
        (value_share - share_lag_13p)      AS share_change_13p,
        (value_share - share_lag_52p)      AS share_change_yoy,
        (numeric_distribution - num_dist_lag_13p) AS numeric_dist_change_13p,
        (weighted_distribution - wgt_dist_lag_13p) AS weighted_dist_change_13p,
        (price_index_vs_category - price_idx_lag_13p) AS price_gap_widening_13p
    FROM nielsen_with_lag
    WHERE share_lag_13p IS NOT NULL  -- Need at least 13 periods of history
""")

# COMMAND ----------
# MAGIC %md ## 2. Normalize Each Component to 0–100

# COMMAND ----------
# Normalization: each component scaled to 0–100 where 100 = maximum competitive risk
# Share loss: most negative change = 100, most positive = 0
# Price gap widening: most widened = 100, narrowed = 0
# Distribution decline: most declined = 100, most gained = 0

# TODO: Implement normalization using portfolio-level min/max or predefined benchmarks
# from model_config.yaml normalization bounds
print("Normalization: TO BE IMPLEMENTED in Phase 5 with category-specific benchmarks")

# COMMAND ----------
# MAGIC %md ## 3. Compute Weighted Composite Score

# COMMAND ----------
# TODO: Apply WEIGHTS dict to normalized components
# Handle NULL components: redistribute weights proportionally if ≤ 2 components NULL
# Return NULL if > 2 components are NULL

# COMMAND ----------
# MAGIC %md ## 4. Apply Alert Logic

# COMMAND ----------
# Alert fires if score > ALERT_THRESHOLD for ALERT_CONSECUTIVE_PERIODS consecutive periods
w = Window.partitionBy("brand_key", "channel_key").orderBy("period_key")
# TODO: Implement consecutive period check using lag window

# COMMAND ----------
# MAGIC %md ## 5. Write to MART (via Snowflake)
# competitive_risk_score, competitive_risk_confidence, competitive_risk_alert_flag
print("Competitive Risk Score v1 computation: structure defined. Implementation in Phase 5.")
