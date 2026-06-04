# Databricks notebook source
# MAGIC %md
# MAGIC # Connection Test — Run This First
# MAGIC
# MAGIC **Purpose:** Verify that Databricks can reach Snowflake before running any profiling.
# MAGIC Run this notebook once. Fix any ❌ before proceeding to notebook 01.
# MAGIC
# MAGIC **No data is read.** This only tests connectivity and secret access.

# COMMAND ----------
# MAGIC %md ## 1. Verify Secret Scope is Accessible

# COMMAND ----------
SCOPE = "MGI_SECRETS"

required_secrets = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_DB_PREFIX",
]

print("Checking Databricks Secret Scope: MGI_SECRETS")
print("=" * 50)
all_ok = True
for key in required_secrets:
    try:
        val = dbutils.secrets.get(scope=SCOPE, key=key)
        # Don't print the value — just confirm it exists and is non-empty
        status = "✅ OK" if val and len(val.strip()) > 0 else "⚠️  EMPTY"
        print(f"  {key:<35} {status}")
    except Exception as e:
        print(f"  {key:<35} ❌ MISSING — {str(e)[:60]}")
        all_ok = False

if not all_ok:
    print("\n❌ Fix missing secrets before continuing.")
    print("   Run: scripts/setup_databricks_secrets_windows.ps1 (Windows)")
    print("   Or:  scripts/setup_databricks_secrets.sh (Mac/Linux)")
    dbutils.notebook.exit("SECRET_SETUP_INCOMPLETE")
else:
    print("\n✅ All secrets found.")

# COMMAND ----------
# MAGIC %md ## 2. Read Connection Parameters

# COMMAND ----------
sf_account   = dbutils.secrets.get(scope=SCOPE, key="SNOWFLAKE_ACCOUNT")
sf_user      = dbutils.secrets.get(scope=SCOPE, key="SNOWFLAKE_USER")
sf_warehouse = dbutils.secrets.get(scope=SCOPE, key="SNOWFLAKE_WAREHOUSE")
sf_role      = dbutils.secrets.get(scope=SCOPE, key="SNOWFLAKE_ROLE")
sf_db_prefix = dbutils.secrets.get(scope=SCOPE, key="SNOWFLAKE_DB_PREFIX")

ENVIRONMENT  = "DEV"                          # Change to STAGING or PROD when ready
SF_DATABASE  = f"{sf_db_prefix}_{ENVIRONMENT}"

# Does the workspace use password or private key?
try:
    sf_private_key = dbutils.secrets.get(scope=SCOPE, key="SNOWFLAKE_PRIVATE_KEY")
    AUTH_METHOD = "private_key"
except:
    try:
        sf_password = dbutils.secrets.get(scope=SCOPE, key="SNOWFLAKE_PASSWORD")
        AUTH_METHOD = "password"
    except:
        AUTH_METHOD = "none"

print(f"Account:    [REDACTED].snowflakecomputing.com")
print(f"User:       [REDACTED]")
print(f"Warehouse:  [REDACTED]")
print(f"Role:       [REDACTED]")
print(f"Database:   {SF_DATABASE}")
print(f"Auth method: {AUTH_METHOD}")

# COMMAND ----------
# MAGIC %md ## 3. Test Snowflake Connection

# COMMAND ----------
# Build Snowflake options dict for Spark connector
sf_options = {
    "sfURL":       f"{sf_account}.snowflakecomputing.com",
    "sfUser":      sf_user,
    "sfWarehouse": sf_warehouse,
    "sfRole":      sf_role,
    "sfDatabase":  SF_DATABASE,
    "sfSchema":    "BRONZE",
}

if AUTH_METHOD == "private_key":
    sf_options["pem_private_key"] = sf_private_key
elif AUTH_METHOD == "password":
    sf_options["sfPassword"] = sf_password
else:
    print("❌ No authentication method found. Run secret setup script.")
    dbutils.notebook.exit("NO_AUTH_METHOD")

print("Testing Snowflake connection...")
try:
    test_df = spark.read \
        .format("snowflake") \
        .options(**sf_options) \
        .option("query", "SELECT CURRENT_TIMESTAMP() AS ts, CURRENT_DATABASE() AS db, CURRENT_WAREHOUSE() AS wh") \
        .load()
    
    row = test_df.collect()[0]
    print(f"\n✅ Connection successful!")
    print(f"   Timestamp:  {row['TS']}")
    print(f"   Database:   {row['DB']}")
    print(f"   Warehouse:  {row['WH']}")
except Exception as e:
    print(f"\n❌ Connection failed: {e}")
    print("\nCommon fixes:")
    print("  1. Check SNOWFLAKE_ACCOUNT format (should be: abc12345.region)")
    print("  2. Verify your Snowflake role has access to the warehouse")
    print("  3. Check that the Snowflake warehouse is running (not suspended)")
    print("  4. Verify private key format (no header/footer lines, single line)")
    dbutils.notebook.exit("CONNECTION_FAILED")

# COMMAND ----------
# MAGIC %md ## 4. Verify Database Exists

# COMMAND ----------
try:
    schemas_df = spark.read \
        .format("snowflake") \
        .options(**sf_options) \
        .option("query", f"SHOW SCHEMAS IN DATABASE {SF_DATABASE}") \
        .load()
    
    schema_names = [row["name"] for row in schemas_df.collect()]
    
    expected = ["BRONZE", "SILVER", "GOLD", "MART", "MONITORING", "FEATURE_STORE"]
    print(f"\nSchemas in {SF_DATABASE}:")
    for s in expected:
        status = "✅" if s in schema_names else "⚠️  MISSING — run scripts/setup_snowflake_schemas.sh"
        print(f"  {s:<20} {status}")
    
    print(f"\nAll schemas found: {', '.join(schema_names)}")

except Exception as e:
    print(f"⚠️  Could not list schemas: {e}")
    print(f"   The database {SF_DATABASE} may not exist yet.")
    print(f"   Run: scripts/setup_snowflake_schemas.sh {ENVIRONMENT.lower()}")

# COMMAND ----------
# MAGIC %md ## 5. Summary

# COMMAND ----------
print("=" * 50)
print("CONNECTION TEST COMPLETE")
print("=" * 50)
print(f"  Secrets:    ✅ All {len(required_secrets)} found")
print(f"  Auth:       ✅ {AUTH_METHOD}")
print(f"  Snowflake:  ✅ Connected")
print("")
print("Next step: Open notebook 01_data_profiling_sell_in.py")
print("Update SOURCE_TABLE to match your actual Bronze table name, then Run All.")
