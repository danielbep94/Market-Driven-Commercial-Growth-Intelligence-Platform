# Databricks notebook source
# MAGIC %md
# MAGIC # gold_dim_channel.py — Gold Channel Dimension Build
# MAGIC ## Enterprise Channel Hierarchy Implementation

# COMMAND ----------

import hashlib
import datetime
from pyspark.sql import functions as F, types as T

RUN_DATE = datetime.date.today().isoformat()

def log(tag, msg, level="INFO"):
    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][{level}][{tag}] {msg}")

log("S0", f"Starting gold_dim_channel.py on {RUN_DATE}")

SEED_DBFS_PATH = "dbfs:/mnt/mdp/mdm/master_catalog/canal/seed/channel_hierarchy_seed.csv"

_SEED_SCHEMA = T.StructType([
    T.StructField("source_system",       T.StringType()),
    T.StructField("source_column",       T.StringType()),
    T.StructField("source_value",        T.StringType()),
    T.StructField("gran_canal_grp",      T.StringType()),
    T.StructField("channel_standard",    T.StringType()),
    T.StructField("chain_standard",      T.StringType()),
    T.StructField("format_standard",     T.StringType()),
    T.StructField("business_route",      T.StringType()),
    T.StructField("mapping_status",      T.StringType()),
    T.StructField("notes",               T.StringType()),
])

try:
    df_seed = spark.read.csv(SEED_DBFS_PATH, header=True, schema=_SEED_SCHEMA)
    log("S1", f"Loaded enterprise seed from {SEED_DBFS_PATH}: {df_seed.count()} rows")
except Exception as e:
    log("S1", f"Failed to load enterprise seed: {e}", "BLOCKER")
    raise e

# Trim and uppercase everything according to target spec
for col in ["source_system", "source_column", "source_value", "gran_canal_grp", 
            "channel_standard", "chain_standard", "format_standard", "business_route", "mapping_status"]:
    df_seed = df_seed.withColumn(col, F.upper(F.trim(F.col(col))))

# Filter for CONFIRMED mapping status and strictly valid target buckets
df_validated = df_seed.filter(
    (F.col("mapping_status") == "CONFIRMED") &
    F.col("gran_canal_grp").isin("UTT", "DTT", "INTERNAL") &
    F.col("channel_standard").isin("MODERNO", "TRADICIONAL", "INTERNOS")
)

# Generate Surrogate Key
def canal_key(sys, col, val, gran, channel, chain, format_std):
    # Match dbt_utils.generate_surrogate_key logic by concatenating values
    raw = "|".join([str(x) for x in [sys, col, val, gran, channel, chain, format_std]])
    return hashlib.md5(raw.encode()).hexdigest()[:16]

udf_canal_key = F.udf(canal_key, T.StringType())

df_dim_channel = df_validated.withColumn(
    "channel_key",
    udf_canal_key(
        F.col("source_system"),
        F.col("source_column"),
        F.col("source_value"),
        F.col("gran_canal_grp"),
        F.col("channel_standard"),
        F.col("chain_standard"),
        F.col("format_standard")
    )
).select(
    "channel_key",
    "source_system",
    "source_column",
    "source_value",
    "gran_canal_grp",
    "channel_standard",
    "chain_standard",
    "format_standard",
    "business_route",
    "notes"
).withColumn("created_at", F.current_timestamp())

# Output
output_path = "dbfs:/mnt/mdp/mdm/gold/dimensions/dim_channel.csv"
try:
    df_dim_channel.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
    log("S2", f"Written {df_dim_channel.count()} rows to {output_path}")
except Exception as e:
    log("S2", f"Write failed: {e}", "WARNING")

log("S9", "gold_dim_channel.py complete.")
