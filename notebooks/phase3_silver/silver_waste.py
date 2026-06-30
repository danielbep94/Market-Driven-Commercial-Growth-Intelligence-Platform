# Databricks notebook source
# MAGIC %md
# MAGIC # silver_waste.py — Waste Data Standardization
# MAGIC ## Enterprise Channel Hierarchy Implementation

# COMMAND ----------

# MAGIC %run ./silver_homologation_apply

# COMMAND ----------

import datetime
from pyspark.sql import functions as F, types as T

RUN_DATE = datetime.date.today().isoformat()

def log(tag, msg, level="INFO"):
    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][{level}][{tag}] {msg}")

log("S0", f"Starting silver_waste.py on {RUN_DATE}")

# Assuming DB_PRD_MDP and run_sf are provided by a common framework or run directly using Spark
# For this script we will use Spark SQL assuming views/tables are mounted or available.

SQL_WASTE_SRC = """
    SELECT
        fecha,
        sku,
        UPPER(TRIM(cadena))  AS cadena_raw,
        UPPER(TRIM(formato)) AS formato_raw,
        UPPER(TRIM(canal))   AS canal_raw,
        UPPER(TRIM(fuente))  AS fuente,
        waste_amount,
        waste_kg
    FROM PRD_MDP.MDP_STG.VW_WASTE
    WHERE UPPER(TRIM(fuente)) = 'TOPLINE'
"""

try:
    # Use Snowflake pushdown instead of spark.sql (Unity Catalog is not enabled)
    df_waste = run_sf(DB_PRD_MDP, SQL_WASTE_SRC)
    log("S1", f"Loaded raw waste data: {df_waste.count()} rows")
except Exception as e:
    log("S1", f"Failed to load raw waste data via Snowflake wrapper: {e}", "BLOCKER")
    raise e

# Apply target enterprise hierarchy
df_waste_mapped = (
    df_waste
    .withColumn(
        "gran_canal_grp",
        F.when(F.col("canal_raw").isin("MODERNO", "NC MODERNO"), "UTT")
         .when(F.col("canal_raw") == "TRADICIONAL", "DTT")
         .when(F.col("canal_raw") == "INTERNOS", "INTERNAL")
         .otherwise(None)
    )
    .withColumn(
        "channel_standard",
        F.when(F.col("canal_raw").isin("MODERNO", "NC MODERNO"), "MODERNO")
         .when(F.col("canal_raw") == "TRADICIONAL", "TRADICIONAL")
         .when(F.col("canal_raw") == "INTERNOS", "INTERNOS")
         .otherwise(None)
    )
    .withColumn(
        "business_route",
        F.when(F.col("canal_raw").isin("MODERNO", "NC MODERNO", "TRADICIONAL"), "COMMERCIAL")
         .when(F.col("canal_raw") == "INTERNOS", "INTERNAL_OPERATION")
         .otherwise(None)
    )
    .withColumn("source_system", F.lit("WASTE"))
)

# Add assertions for data contract validation (Rule 5)
assert_df = df_waste_mapped.select("channel_standard", "gran_canal_grp", "business_route").distinct().toPandas()

channel_std_set = set(assert_df["channel_standard"].dropna())
assert channel_std_set <= {"MODERNO", "TRADICIONAL", "INTERNOS"}, f"Invalid channel_standard found: {channel_std_set}"

gran_canal_grp_set = set(assert_df["gran_canal_grp"].dropna())
assert gran_canal_grp_set <= {"UTT", "DTT", "INTERNAL"}, f"Invalid gran_canal_grp found: {gran_canal_grp_set}"

null_business_routes = df_waste_mapped.filter(F.col("business_route").isNull()).count()
assert null_business_routes == 0, f"Found {null_business_routes} rows with missing business_route"

log("S2", "Enterprise channel hierarchy standard applied and assertions passed successfully.")

# Save output to DBFS or table
output_path = "dbfs:/mnt/mdp/mdm/silver/waste/waste_std.csv"
try:
    df_waste_mapped.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
    log("S3", f"Waste standardized output saved to {output_path}")
except Exception as e:
    log("S3", f"Could not save waste output to DBFS, might be a table write instead: {e}", "WARNING")

log("S9", "silver_waste.py complete.")
