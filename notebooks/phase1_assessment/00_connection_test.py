# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Connection Test — Run This First
# MAGIC
# MAGIC **Purpose:** Verify Azure Key Vault secrets are accessible and Snowflake connection works.
# MAGIC
# MAGIC **Run once before any profiling notebook. Fix any ❌ before proceeding.**
# MAGIC
# MAGIC No data is read from production tables — only connectivity is tested.

# COMMAND ----------
# MAGIC %md ## Configuration
# MAGIC
# MAGIC Update `DB_NAME` and `SCHEMA_NAME` to point to your source data in Snowflake.
# MAGIC These are the **source tables** you want to profile in Phase 1.

# COMMAND ----------
# ── UPDATE THESE to match your Snowflake environment ────────────────────────
DB_NAME     = "YOUR_SOURCE_DATABASE"   # e.g. "MDP_PRD" or "DANONE_DW"
SCHEMA_NAME = "YOUR_SOURCE_SCHEMA"     # e.g. "COMMERCIAL" or "SELL_IN"

# ── Azure Key Vault scope — do not change ───────────────────────────────────
KEYVAULT_NAME = "DAN-AM-P-KVT800-R-MDP-DB"
KEY_NAME_USR  = "snowflake-user"
KEY_NAME_PWD  = "snowflake-password"

SF_URL        = "danonenam.east-us-2.azure.snowflakecomputing.com"
SF_WAREHOUSE  = "PRD_MDP_ANL_WH"

# COMMAND ----------
# MAGIC %md ## 1. Retrieve Secrets from Azure Key Vault

# COMMAND ----------
print(f"Retrieving secrets from scope: {KEYVAULT_NAME}")
print("-" * 50)

try:
    user     = dbutils.secrets.get(scope=KEYVAULT_NAME, key=KEY_NAME_USR)
    password = dbutils.secrets.get(scope=KEYVAULT_NAME, key=KEY_NAME_PWD)
    print(f"  ✅ {KEY_NAME_USR}   — retrieved")
    print(f"  ✅ {KEY_NAME_PWD}  — retrieved")
    SECRETS_OK = True
except NameError:
    print("  🚨 Not running in Databricks. Using MOCK credentials.")
    user, password = "MOCK_USER", "MOCK_PASSWORD"
    SECRETS_OK = False
except Exception as e:
    print(f"  ❌ Secret retrieval failed: {e}")
    print(f"\n  Fix: verify the Azure Key Vault '{KEYVAULT_NAME}' is linked to this Databricks workspace.")
    print(f"  Check: Settings → Admin Console → Secret Scopes → {KEYVAULT_NAME}")
    SECRETS_OK = False
    dbutils.notebook.exit("SECRET_RETRIEVAL_FAILED")

# COMMAND ----------
# MAGIC %md ## 2. Build Connection Options

# COMMAND ----------
sfOptions = {
    "sfURL":       SF_URL,
    "sfUser":      user,
    "sfPassword":  password,
    "sfDatabase":  DB_NAME,
    "sfSchema":    SCHEMA_NAME,
    "sfWarehouse": SF_WAREHOUSE,
}

print("Connection parameters:")
print(f"  sfURL:       {SF_URL}")
print(f"  sfUser:      [REDACTED]")
print(f"  sfPassword:  [REDACTED]")
print(f"  sfDatabase:  {DB_NAME}")
print(f"  sfSchema:    {SCHEMA_NAME}")
print(f"  sfWarehouse: {SF_WAREHOUSE}")

# COMMAND ----------
# MAGIC %md ## 3. Test Snowflake Connection

# COMMAND ----------
print("Testing Snowflake connection...")

try:
    test_df = spark.read \
        .format("snowflake") \
        .options(**sfOptions) \
        .option("query", "SELECT CURRENT_TIMESTAMP() AS ts, CURRENT_DATABASE() AS db, CURRENT_WAREHOUSE() AS wh, CURRENT_USER() AS usr") \
        .load()

    row = test_df.collect()[0]
    print(f"\n  ✅ Connection successful!")
    print(f"     Timestamp:  {row['TS']}")
    print(f"     Database:   {row['DB']}")
    print(f"     Warehouse:  {row['WH']}")
    print(f"     User:       [REDACTED]")
    CONN_OK = True

except Exception as e:
    err = str(e)
    print(f"\n  ❌ Connection failed.")
    print(f"\n  Error: {err[:300]}")
    print("\n  Common fixes:")
    print("    1. Is DB_NAME correct? It must exist in Snowflake.")
    print("    2. Is the warehouse PRD_MDP_ANL_WH running (not suspended)?")
    print("    3. Does the Snowflake user have USAGE on that warehouse and database?")
    print("    4. Is the Snowflake Spark connector installed on this cluster?")
    print("       (Cluster Libraries → Install → Maven → net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.3)")
    CONN_OK = False
    dbutils.notebook.exit("CONNECTION_FAILED")

# COMMAND ----------
# MAGIC %md ## 4. Check Available Schemas in Source Database

# COMMAND ----------
print(f"Listing schemas in {DB_NAME}:")
try:
    schemas_df = spark.read \
        .format("snowflake") \
        .options(**sfOptions) \
        .option("query", f"SHOW SCHEMAS IN DATABASE {DB_NAME}") \
        .load()
    schemas_df.select("name").show(30, truncate=False)
except Exception as e:
    print(f"  ⚠️  Could not list schemas: {str(e)[:200]}")
    print(f"     Possible reason: user lacks SHOW SCHEMAS privilege on {DB_NAME}")

# COMMAND ----------
# MAGIC %md ## 5. Quick Table Discovery (Optional)

# COMMAND ----------
print(f"Listing tables in {DB_NAME}.{SCHEMA_NAME}:")
try:
    tables_df = spark.read \
        .format("snowflake") \
        .options(**sfOptions) \
        .option("query", f"SHOW TABLES IN {DB_NAME}.{SCHEMA_NAME}") \
        .load()
    tables_df.select("name", "rows", "created_on").show(50, truncate=False)
except Exception as e:
    print(f"  ⚠️  Could not list tables: {str(e)[:200]}")
    print(f"     Update SCHEMA_NAME to a schema that exists in {DB_NAME}")

# COMMAND ----------
# MAGIC %md ## 6. Result

# COMMAND ----------
print("=" * 55)
print("CONNECTION TEST RESULT")
print("=" * 55)
if SECRETS_OK and CONN_OK:
    print("  ✅ Azure Key Vault secrets: OK")
    print("  ✅ Snowflake connection: OK")
    print("")
    print("  → Ready to run notebook 01_data_profiling_sell_in.py")
    print("")
    print("  ACTION REQUIRED before notebook 01:")
    print(f"  Update DB_NAME and SOURCE_TABLE in notebook 01")
    print(f"  to point to your sell-in source table in Snowflake.")
else:
    print("  ❌ Fix the errors above before continuing.")
print("=" * 55)
