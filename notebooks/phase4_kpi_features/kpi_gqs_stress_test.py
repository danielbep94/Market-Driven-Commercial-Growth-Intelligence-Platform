# Databricks notebook source
# Phase 4 — Growth Quality Score Stress Test
# Run after Phase 4 KPI engineering is complete.
# Tests two failure modes:
#   1. Score compression — all brands cluster between 55-70 (IQR < 15 pts)
#   2. Score stability — week-to-week volatility > 10 pts for typical brands
#
# Output: docs/phase_outputs/phase4_gqs_stress_test.md
# Decision gate: If IQR < 15 pts → recalibrate weights before Phase 5 begins.
# This gate is non-negotiable.

# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.window import Window

ENVIRONMENT = "prod"  # Run stress test on full history in prod/staging
SNOWFLAKE_DB = f"MGI_{ENVIRONMENT.upper()}"
HISTORY_MONTHS = 12   # Minimum 12 months for meaningful distribution

# COMMAND ----------
# MAGIC %md ## 1. Load GQS History (12+ months)

# COMMAND ----------
df_gqs = spark.sql(f"""
    SELECT
        brand_key,
        brand_name,
        channel_key,
        channel_name,
        month_key,
        year_number,
        month_number,
        growth_quality_score,
        gqs_score_4w_avg,
        gqs_confidence_level
    FROM {SNOWFLAKE_DB}.MART.MART_MARKET_GROWTH_INTELLIGENCE_MONTHLY
    WHERE growth_quality_score IS NOT NULL
      AND gqs_confidence_level IN ('HIGH', 'MEDIUM')
      AND month_key >= CAST(DATE_TRUNC('month', DATEADD('month', -{HISTORY_MONTHS}, CURRENT_DATE())) AS VARCHAR(6))
    ORDER BY brand_key, channel_key, month_key
""")

print(f"Records loaded: {df_gqs.count()}")
print(f"Unique brands: {df_gqs.select('brand_key').distinct().count()}")

# COMMAND ----------
# MAGIC %md ## 2. Stress Test 1 — Score Compression

# COMMAND ----------
gqs_stats = df_gqs.agg(
    F.percentile_approx("growth_quality_score", 0.25).alias("p25"),
    F.percentile_approx("growth_quality_score", 0.75).alias("p75"),
    F.percentile_approx("growth_quality_score", 0.10).alias("p10"),
    F.percentile_approx("growth_quality_score", 0.90).alias("p90"),
    F.mean("growth_quality_score").alias("mean"),
    F.stddev("growth_quality_score").alias("stddev"),
    F.min("growth_quality_score").alias("min"),
    F.max("growth_quality_score").alias("max"),
).collect()[0]

iqr = gqs_stats["p75"] - gqs_stats["p25"]
print(f"\n=== GQS Distribution (last {HISTORY_MONTHS} months) ===")
print(f"P10: {gqs_stats['p10']:.1f}")
print(f"P25 (Q1): {gqs_stats['p25']:.1f}")
print(f"Mean: {gqs_stats['mean']:.1f}")
print(f"P75 (Q3): {gqs_stats['p75']:.1f}")
print(f"P90: {gqs_stats['p90']:.1f}")
print(f"IQR (P75-P25): {iqr:.1f}")
print(f"Std Dev: {gqs_stats['stddev']:.1f}")
print(f"Min: {gqs_stats['min']:.1f} | Max: {gqs_stats['max']:.1f}")

IQR_THRESHOLD = 15  # From configs/dq_thresholds.yaml: gqs_iqr_minimum_threshold
if iqr < IQR_THRESHOLD:
    print(f"\n⚠️  COMPRESSION DETECTED: IQR = {iqr:.1f} < {IQR_THRESHOLD}")
    print("ACTION REQUIRED: Recalibrate GQS weights before Phase 5 begins.")
    print("Update configs/kpi_weights.yaml with new version; rerun this notebook to confirm.")
    dbutils.notebook.exit("RECALIBRATION_REQUIRED")
else:
    print(f"\n✅ Score distribution acceptable: IQR = {iqr:.1f} >= {IQR_THRESHOLD}")

# COMMAND ----------
# MAGIC %md ## 3. Stress Test 2 — Week-to-Week Volatility

# COMMAND ----------
w = Window.partitionBy("brand_key", "channel_key").orderBy("month_key")
df_volatility = (
    df_gqs
    .withColumn("gqs_prior_month", F.lag("growth_quality_score").over(w))
    .withColumn("gqs_mom_change", F.abs(F.col("growth_quality_score") - F.col("gqs_prior_month")))
    .filter(F.col("gqs_prior_month").isNotNull())
)

volatility_stats = df_volatility.agg(
    F.mean("gqs_mom_change").alias("avg_mom_change"),
    F.percentile_approx("gqs_mom_change", 0.90).alias("p90_mom_change"),
    F.max("gqs_mom_change").alias("max_mom_change"),
).collect()[0]

print(f"\n=== GQS Month-over-Month Volatility ===")
print(f"Average MoM change: {volatility_stats['avg_mom_change']:.1f} pts")
print(f"P90 MoM change: {volatility_stats['p90_mom_change']:.1f} pts")
print(f"Max MoM change: {volatility_stats['max_mom_change']:.1f} pts")

if volatility_stats["p90_mom_change"] > 10:
    print(f"\n⚠️  HIGH VOLATILITY: P90 MoM change = {volatility_stats['p90_mom_change']:.1f} pts > 10")
    print("The 4-week smoothed GQS (gqs_score_4w_avg) is confirmed as necessary for Recommendation Engine.")
    print("Verify gqs_score_4w_avg is being used in recommendation_engine/02_recommendation_logic.py")
else:
    print(f"\n✅ Volatility acceptable: P90 = {volatility_stats['p90_mom_change']:.1f} pts")

# COMMAND ----------
# MAGIC %md ## 4. Write Stress Test Report
# TODO: Write summary results to docs/phase_outputs/phase4_gqs_stress_test.md
# Include: IQR value, volatility stats, pass/fail status, weight version used
print("\nStress test complete. Document results in docs/phase_outputs/phase4_gqs_stress_test.md")
