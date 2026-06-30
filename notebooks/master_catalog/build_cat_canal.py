# Databricks notebook source
# MAGIC %md
# MAGIC # build_cat_canal.py — CANAL Catalog Build (Enterprise Hierarchy)
# MAGIC ## Danone Master Data Catalog v7.0.0
# MAGIC ## Enterprise Channel Hierarchy Standardization

# COMMAND ----------

import re, hashlib, datetime, os
from pyspark.sql import functions as F, types as T

CATALOG_VERSION   = "7.0.0"
RUN_DATE          = datetime.date.today().isoformat()
DBFS_BASE         = "dbfs:/mnt/mdp/mdm/master_catalog/canal"
SEED_DBFS_PATH    = "dbfs:/mnt/mdp/mdm/master_catalog/canal/seed/channel_hierarchy_seed.csv"
SEED_REPO_PATH    = "/Workspace/Users/victor.hernandez29@danone.com/Market-Driven-Commercial-Growth-Intelligence-Platform/configs/catalog_seeds/channel_hierarchy_seed.csv"

def log(tag, msg, level="INFO"):
    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][{level}][{tag}] {msg}")

log("S0", f"build_cat_canal v{CATALOG_VERSION} initializing — run_date={RUN_DATE}")

for sub in ["", "/seed"]:
    dbutils.fs.mkdirs(DBFS_BASE + sub)

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

# Copy seed from repo workspace path to DBFS so Spark can read it
dbutils.fs.cp(f"file:{SEED_REPO_PATH}", SEED_DBFS_PATH, recurse=False)
df_seed = spark.read.csv(SEED_DBFS_PATH, header=True, schema=_SEED_SCHEMA)
log("S1", f"Enterprise seed loaded: {df_seed.count()} rows")

# Filter only confirmed mappings for the target enterprise model
df_confirmed = df_seed.filter(
    (F.col("mapping_status") == "CONFIRMED") & 
    F.col("gran_canal_grp").isin("UTT", "DTT", "INTERNAL") &
    F.col("channel_standard").isin("MODERNO", "TRADICIONAL", "INTERNOS")
)

# Generate Surrogate Key
def canal_key(sys, col, val):
    raw = f"{sys}|{col}|{val}"
    return hashlib.md5(raw.encode()).hexdigest()[:16]

udf_canal_key = F.udf(canal_key, T.StringType())

df_catalog = (
    df_confirmed
    .withColumn(
        "channel_key", 
        udf_canal_key(F.col("source_system"), F.col("source_column"), F.col("source_value"))
    )
    .withColumn("catalog_date", F.lit(RUN_DATE))
    .withColumn("catalog_version", F.lit(CATALOG_VERSION))
    .select(
        "channel_key",
        "source_system",
        "source_column",
        "source_value",
        "gran_canal_grp",
        "channel_standard",
        "chain_standard",
        "format_standard",
        "business_route",
        "notes",
        "catalog_date",
        "catalog_version"
    )
)

out_path = f"{DBFS_BASE}/cat_canal.csv"
df_catalog.coalesce(1).write.mode("overwrite").option("header", True).csv(out_path)

log("S2", f"Enterprise Master Catalog written to {out_path} with {df_catalog.count()} rows")
log("S2", "Build complete.")
