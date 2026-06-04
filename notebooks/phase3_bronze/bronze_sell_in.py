# Databricks notebook source
# Bronze — Sell-in Ingestion
# Loads raw sell-in data to Bronze layer.
# Rules:
#   - Append-only: NEVER overwrite or modify Bronze records
#   - Preserve original column names and values
#   - Add audit metadata: ingestion_timestamp, source_file_name, batch_id
#   - Must be idempotent: running twice produces the same result

# COMMAND ----------
import pyspark.sql.functions as F
from datetime import datetime
import hashlib

# COMMAND ----------
# Configuration
ENVIRONMENT = dbutils.widgets.get("environment") if dbutils.widgets else "dev"
SOURCE_FILE_PATH = dbutils.widgets.get("source_file_path") if dbutils.widgets else ""
BATCH_ID = dbutils.widgets.get("batch_id") if dbutils.widgets else f"manual_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
TARGET_TABLE = f"mgi_{ENVIRONMENT}.bronze.sell_in"

# COMMAND ----------
# MAGIC %md ## 1. Load Raw File

# COMMAND ----------
# Load raw data — preserve ALL original columns and values
df_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")  # Never infer schema in Bronze — everything is string
    .csv(SOURCE_FILE_PATH)
)

print(f"Raw file loaded: {df_raw.count()} rows, {len(df_raw.columns)} columns")
print(f"Columns: {df_raw.columns}")

# COMMAND ----------
# MAGIC %md ## 2. Add Audit Metadata

# COMMAND ----------
df_bronze = (
    df_raw
    .withColumn("_ingestion_timestamp", F.current_timestamp())
    .withColumn("_source_file_name", F.lit(SOURCE_FILE_PATH.split("/")[-1] if SOURCE_FILE_PATH else "UNKNOWN"))
    .withColumn("_batch_id", F.lit(BATCH_ID))
    .withColumn("_environment", F.lit(ENVIRONMENT))
)

# COMMAND ----------
# MAGIC %md ## 3. Duplicate Detection

# COMMAND ----------
# Check if this batch_id has already been loaded (idempotency check)
existing_batches = spark.sql(f"""
    SELECT COUNT(*) as cnt FROM {TARGET_TABLE}
    WHERE _batch_id = '{BATCH_ID}'
""").collect()[0]["cnt"] if spark.catalog.tableExists(TARGET_TABLE) else 0

if existing_batches > 0:
    print(f"WARNING: batch_id '{BATCH_ID}' already exists in Bronze ({existing_batches} rows). Skipping to prevent duplicates.")
    dbutils.notebook.exit("SKIPPED_DUPLICATE_BATCH")

# COMMAND ----------
# MAGIC %md ## 4. Write to Bronze (Append-only)

# COMMAND ----------
(
    df_bronze
    .write
    .mode("append")  # ALWAYS append — never overwrite Bronze
    .saveAsTable(TARGET_TABLE)
)
print(f"Bronze write complete: {df_bronze.count()} rows appended to {TARGET_TABLE}")
print(f"Batch ID: {BATCH_ID}")
