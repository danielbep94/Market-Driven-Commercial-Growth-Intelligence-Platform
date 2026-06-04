# Databricks notebook source

# ── Azure Key Vault Connection ────────────────────────────────────────────────
KEYVAULT_NAME = "DAN-AM-P-KVT800-R-MDP-DB"
KEY_NAME_USR  = "snowflake-user"
KEY_NAME_PWD  = "snowflake-password"
SF_URL        = "danonenam.east-us-2.azure.snowflakecomputing.com"
SF_WAREHOUSE  = "PRD_MDP_ANL_WH"

try:
    user     = dbutils.secrets.get(scope=KEYVAULT_NAME, key=KEY_NAME_USR)
    password = dbutils.secrets.get(scope=KEYVAULT_NAME, key=KEY_NAME_PWD)
    print(f"✅ Credentials loaded from: {KEYVAULT_NAME}")
except NameError:
    print("\n🚨 WARNING: Non-Databricks env. Using MOCK credentials.")
    user = "MOCK_USER"
    password = "MOCK_PASSWORD"
except Exception as e:
    print(f"\n🚨 WARNING: Could not retrieve secrets: {e}. Using MOCK credentials.")
    user = "MOCK_USER"
    password = "MOCK_PASSWORD"

def get_sf_options(db_name, schema_name="PUBLIC"):
    return {
        "sfURL": SF_URL, "sfUser": user, "sfPassword": password,
        "sfDatabase": db_name, "sfSchema": schema_name, "sfWarehouse": SF_WAREHOUSE,
    }

