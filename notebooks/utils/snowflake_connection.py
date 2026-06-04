# Databricks notebook source
# MAGIC %md
# MAGIC # Snowflake Connection Utility
# MAGIC
# MAGIC **Import this at the top of every notebook with:**
# MAGIC ```python
# MAGIC %run ../utils/snowflake_connection
# MAGIC ```
# MAGIC (adjust relative path based on notebook location)
# MAGIC
# MAGIC After running, `get_sf_options(db_name, schema_name)` is available.

# COMMAND ----------
# ── Azure Key Vault — Databricks Secret Scope ────────────────────────────────
KEYVAULT_NAME = "DAN-AM-P-KVT800-R-MDP-DB"
KEY_NAME_USR  = "snowflake-user"
KEY_NAME_PWD  = "snowflake-password"

SF_URL        = "danonenam.east-us-2.azure.snowflakecomputing.com"
SF_WAREHOUSE  = "PRD_MDP_ANL_WH"
SF_ROLE       = "PRD_MDP"

# COMMAND ----------
try:
    _sf_user     = dbutils.secrets.get(scope=KEYVAULT_NAME, key=KEY_NAME_USR)
    _sf_password = dbutils.secrets.get(scope=KEYVAULT_NAME, key=KEY_NAME_PWD)
    print(f"✅ Snowflake credentials loaded from Azure Key Vault scope: {KEYVAULT_NAME}")
except NameError:
    print("🚨 WARNING: Non-Databricks environment. Using MOCK credentials.")
    _sf_user     = "MOCK_USER"
    _sf_password = "MOCK_PASSWORD"
except Exception as e:
    print(f"🚨 WARNING: Could not retrieve secrets: {e}. Using MOCK credentials.")
    _sf_user     = "MOCK_USER"
    _sf_password = "MOCK_PASSWORD"

# COMMAND ----------
def get_sf_options(db_name: str, schema_name: str = "PUBLIC") -> dict:
    """
    Returns the sfOptions dict for the Snowflake Spark connector.

    Usage:
        sfOptions = get_sf_options("MY_DATABASE", "MY_SCHEMA")
        df = spark.read.format("snowflake").options(**sfOptions) \\
                  .option("dbtable", "MY_TABLE").load()
    """
    return {
        "sfURL":       SF_URL,
        "sfUser":      _sf_user,
        "sfPassword":  _sf_password,
        "sfDatabase":  db_name,
        "sfSchema":    schema_name,
        "sfWarehouse": SF_WAREHOUSE,
        "sfRole":      SF_ROLE,
    }


def read_sf_table(db_name: str, schema_name: str, table_name: str):
    """Read a full Snowflake table into a Spark DataFrame."""
    opts = get_sf_options(db_name, schema_name)
    return spark.read.format("snowflake").options(**opts) \
               .option("dbtable", table_name).load()


def read_sf_query(db_name: str, schema_name: str, query: str):
    """Execute a SQL query against Snowflake and return a Spark DataFrame."""
    opts = get_sf_options(db_name, schema_name)
    return spark.read.format("snowflake").options(**opts) \
               .option("query", query).load()


print(f"Snowflake utility ready.")
print(f"  URL:       {SF_URL}")
print(f"  Warehouse: {SF_WAREHOUSE}")
print(f"  Role:      {SF_ROLE}")
print(f"  User:      [REDACTED]")
print(f"\nUsage: sfOptions = get_sf_options('MY_DB', 'MY_SCHEMA')")
