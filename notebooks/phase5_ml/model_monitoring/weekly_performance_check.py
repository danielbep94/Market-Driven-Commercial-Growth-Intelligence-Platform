# Databricks notebook source
# Phase 5 — Weekly Model Performance Check
# Runs every Monday after Gold layer is written to Snowflake.
# Reads predictions from MODEL_METADATA and actuals from FACT_SELL_OUT.
# Writes to MODEL_PERFORMANCE_LOG.
# Fires alert if any retraining trigger threshold is crossed.
#
# This notebook is what makes retraining governance enforceable.
# Without it, the "WAPE degrades > 5pp" trigger is manual and will not happen reliably.

# COMMAND ----------
import pyspark.sql.functions as F
from datetime import datetime, timedelta
import yaml

# COMMAND ----------
# Configuration
ENVIRONMENT = dbutils.widgets.get("environment") if dbutils.widgets else "prod"
EVALUATION_WEEK = dbutils.widgets.get("evaluation_week") if dbutils.widgets else None  # ISO date, default last complete week
SNOWFLAKE_DB = f"MGI_{ENVIRONMENT.upper()}"
MONITORING_SCHEMA = "MONITORING"

# Load DQ thresholds from config
# In production, these are read from the YAML config file mounted on the cluster
WAPE_DEGRADATION_THRESHOLD_PP = 5.0   # From configs/dq_thresholds.yaml
RECALL_DROP_THRESHOLD = 0.10           # From configs/dq_thresholds.yaml

# COMMAND ----------
# MAGIC %md ## 1. Determine Evaluation Week

# COMMAND ----------
if EVALUATION_WEEK is None:
    # Default: last complete ISO week (Monday to Sunday)
    today = datetime.utcnow().date()
    days_since_monday = today.weekday()
    last_monday = today - timedelta(days=days_since_monday + 7)
    EVALUATION_WEEK = last_monday.isoformat()

print(f"Evaluating model performance for week starting: {EVALUATION_WEEK}")

# COMMAND ----------
# MAGIC %md ## 2. Load Model Baselines from MODEL_METADATA

# COMMAND ----------
df_model_metadata = spark.sql(f"""
    SELECT
        model_name,
        model_version,
        channel,
        wape_at_training,
        recall_at_training,
        training_date,
        is_champion
    FROM {SNOWFLAKE_DB}.MONITORING.MODEL_METADATA
    WHERE is_champion = TRUE
""")

print(f"Active champion models: {df_model_metadata.count()}")
df_model_metadata.show()

# COMMAND ----------
# MAGIC %md ## 3. Compute WAPE for Each Model This Week
#
# WAPE = SUM(|forecast - actual|) / SUM(actual)
# Computed per model (channel) using FACT_FORECAST and FACT_SELL_OUT

# COMMAND ----------
df_wape_current = spark.sql(f"""
    WITH forecast_vs_actual AS (
        SELECT
            f.brand_key,
            f.channel_key,
            f.week_key,
            f.forecasted_units,
            s.units_sold AS actual_units,
            ch.channel_type
        FROM {SNOWFLAKE_DB}.GOLD.FACT_FORECAST f
        JOIN {SNOWFLAKE_DB}.GOLD.FACT_SELL_OUT s
            ON f.brand_key = s.brand_key
           AND f.channel_key = s.channel_key
           AND f.week_key = s.week_key
        JOIN {SNOWFLAKE_DB}.GOLD.DIM_CHANNEL ch
            ON f.channel_key = ch.channel_key
        JOIN {SNOWFLAKE_DB}.GOLD.DIM_DATE d
            ON f.week_key = d.date_key
        WHERE d.full_date = '{EVALUATION_WEEK}'
          AND f.is_official_version = TRUE   -- Only the period-close frozen version
          AND s.is_reportable = TRUE
          AND s.actual_units > 0             -- Exclude zero-actual periods from WAPE
    )
    SELECT
        channel_type,
        SUM(ABS(forecasted_units - actual_units)) / NULLIF(SUM(actual_units), 0) AS wape_actual,
        COUNT(DISTINCT brand_key) AS n_brands_evaluated,
        COUNT(*) AS n_skus_evaluated
    FROM forecast_vs_actual
    GROUP BY channel_type
""")

df_wape_current.show()

# COMMAND ----------
# MAGIC %md ## 4. Compare Against Baselines and Detect Triggers

# COMMAND ----------
# TODO: Join df_wape_current with df_model_metadata on channel_type
# Compute wape_delta_pp = wape_actual - wape_at_training
# Flag: wape_trigger_fired = (wape_delta_pp > WAPE_DEGRADATION_THRESHOLD_PP)

# COMMAND ----------
# MAGIC %md ## 5. Write to MODEL_PERFORMANCE_LOG

# COMMAND ----------
# TODO: Write results (including trigger flags) to MODEL_PERFORMANCE_LOG
# Use append mode — never overwrite this table

# COMMAND ----------
# MAGIC %md ## 6. Fire Alerts if Triggers Detected

# COMMAND ----------
# TODO: For any row where wape_trigger_fired = TRUE or recall_trigger_fired = TRUE:
# 1. Send alert via Databricks notification or email
# 2. Update MODEL_PERFORMANCE_LOG: alert_sent = TRUE, alert_recipient = [config]
# 3. Log: "RETRAINING TRIGGERED for [model_name] — WAPE degraded [X]pp above baseline"
print("Weekly model performance check complete.")
print("Review MODEL_PERFORMANCE_LOG for trigger results.")
