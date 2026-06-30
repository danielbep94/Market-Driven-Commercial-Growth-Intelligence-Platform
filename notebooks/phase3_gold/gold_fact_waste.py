# Databricks notebook source
# MAGIC %md
# MAGIC # gold_fact_waste.py — Gold Waste Fact Table Build
# MAGIC ## Enterprise Channel Hierarchy Implementation

# COMMAND ----------

import hashlib
import datetime
from pyspark.sql import functions as F, types as T

RUN_DATE = datetime.date.today().isoformat()

def log(tag, msg, level="INFO"):
    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][{level}][{tag}] {msg}")

log("S0", f"Starting gold_fact_waste.py on {RUN_DATE}")

SILVER_WASTE_PATH = "dbfs:/mnt/mdp/mdm/silver/waste/waste_std.csv"
DIM_CHANNEL_PATH  = "dbfs:/mnt/mdp/mdm/gold/dimensions/dim_channel.csv"

try:
    df_waste_std = spark.read.csv(SILVER_WASTE_PATH, header=True)
    log("S1", f"Loaded silver waste: {df_waste_std.count()} rows")
except Exception as e:
    log("S1", f"Failed to load silver waste data: {e}", "BLOCKER")
    raise e

try:
    df_dim_channel = spark.read.csv(DIM_CHANNEL_PATH, header=True)
    log("S2", f"Loaded gold dim_channel: {df_dim_channel.count()} rows")
except Exception as e:
    log("S2", f"Failed to load gold dim_channel: {e}", "BLOCKER")
    raise e

# Join waste to dim_channel to get channel_key based on natural keys
# natural key: source_system, source_column, source_value, gran_canal_grp, channel_standard, business_route

df_fact_waste = df_waste_std.join(
    df_dim_channel.select(
        "channel_key",
        "source_system",
        "source_value",
        "gran_canal_grp",
        "channel_standard",
        "business_route"
    ),
    (df_waste_std["source_system"] == df_dim_channel["source_system"]) &
    (df_waste_std["canal_raw"] == df_dim_channel["source_value"]) &
    (df_waste_std["gran_canal_grp"] == df_dim_channel["gran_canal_grp"]) &
    (df_waste_std["channel_standard"] == df_dim_channel["channel_standard"]) &
    (df_waste_std["business_route"] == df_dim_channel["business_route"]),
    "left"
)

# Select relevant columns for the fact table
df_fact_waste_out = df_fact_waste.select(
    "fecha",
    "sku",
    "channel_key",
    "cadena_raw",
    "formato_raw",
    "canal_raw",
    "fuente",
    "waste_amount",
    "waste_kg",
    "business_route", # Exposing business route explicitly on the fact
    "gran_canal_grp",
    "channel_standard"
).withColumn("created_at", F.current_timestamp())

# Validation: ensure no missing channel keys
null_keys = df_fact_waste_out.filter(F.col("channel_key").isNull()).count()
if null_keys > 0:
    log("S3", f"Warning: {null_keys} waste rows did not match dim_channel.", "WARNING")
else:
    log("S3", "All waste rows successfully matched to dim_channel.")

# Output
output_path = "dbfs:/mnt/mdp/mdm/gold/facts/fact_waste.csv"
try:
    df_fact_waste_out.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
    log("S4", f"Written {df_fact_waste_out.count()} rows to {output_path}")
except Exception as e:
    log("S4", f"Write failed: {e}", "WARNING")

log("S9", "gold_fact_waste.py complete.")
