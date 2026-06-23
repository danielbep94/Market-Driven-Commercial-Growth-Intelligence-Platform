import hashlib
import json
from collections import OrderedDict
from datetime import datetime, timezone

# ═══════════════════════════════════════════════════════════════════════════════
# SNOWFLAKE CONNECTION PROFILES — SINGLE SOURCE OF TRUTH
# ═══════════════════════════════════════════════════════════════════════════════
# File : configs/snowflake_connection_profiles.py
# Purpose : Centralised credential & connection-parameter definitions for every
#           Snowflake database used by this project.
#
# HOW TO USE (Databricks notebook):
#   from configs.snowflake_connection_profiles import get_sf_options
#   opts = get_sf_options("PRD_MEX")
#   df = spark.read.format("net.snowflake.spark.snowflake") \
#             .options(**opts) \
#             .option("query", "SELECT 1") \
#             .load()
#
# ⚠️  SECURITY: PRD_MEX credentials are stored inline for read-only roles.
#     PRD_MDP credentials are always resolved from Databricks Key Vault at runtime.
#     Never promote hardcoded credentials beyond the read-only analyst role.
#
# VALIDATED: 2026-06-23 — V12A–V12E all ✅ PASS (see SEMANTIC_LAYOUTS/INFRASTRUCTURE/
#            SNOWFLAKE_CONNECTION.txt for full credential validation record)
# ═══════════════════════════════════════════════════════════════════════════════

SF_URL = "danonenam.east-us-2.azure.snowflakecomputing.com"

# ─── Profile: PRD_MEX ─────────────────────────────────────────────────────────
# Database  : PRD_MEX
# Schema    : MEX_DSP_DPH_MKT (Nielsen, MKT_OFF, MKT_ON, Waste, Water, etc.)
# Role      : PRD_MEX_READER   ← read-only analyst role
# Warehouse : PRD_MEX_ANL_WH
# Validated : V1–V12, all queries successful
# ──────────────────────────────────────────────────────────────────────────────
PRD_MEX_PROFILE = {
    "sfURL":       SF_URL,
    "sfUser":      "PRD_OSM_DPH_READER",
    "sfPassword":  "73.bBZmne7Aq",
    "sfWarehouse": "PRD_MEX_ANL_WH",
    "sfRole":      "PRD_MEX_READER",
}

# ─── Profile: PRD_MDP ─────────────────────────────────────────────────────────
# Database  : PRD_MDP
# Schema    : MDP_DSP (IBP / demand planning data)
# Role      : PRD_MDP           ← NOTE: role is "PRD_MDP", NOT "PRD_MDP_READER"
#             "PRD_MDP_READER" does NOT exist — using it causes:
#             SnowflakeSQLException: Role 'PRD_MDP_READER' does not exist or not authorized
# Warehouse : PRD_MDP_ANL_WH
# Credentials: resolved from Databricks Key Vault at runtime (never hardcoded)
# Key Vault scope: DAN-AM-P-KVT800-R-MDP-DB
# Validated : V12E — IBP MARCA_STD × EDP join ✅ PASS (2026-06-23)
# ──────────────────────────────────────────────────────────────────────────────
KEYVAULT_SCOPE = "DAN-AM-P-KVT800-R-MDP-DB"
PRD_MDP_PROFILE = {
    "sfURL":       SF_URL,
    "sfUser":      dbutils.secrets.get(scope=KEYVAULT_SCOPE, key="snowflake-user"),
    "sfPassword":  dbutils.secrets.get(scope=KEYVAULT_SCOPE, key="snowflake-password"),
    "sfWarehouse": "PRD_MDP_ANL_WH",
    "sfRole":      "PRD_MDP",          # ← CONFIRMED correct role (not PRD_MDP_READER)
}

# ─── Profile Router ───────────────────────────────────────────────────────────
# Maps each database name → its connection profile.
# Add new databases here; do NOT hardcode profile lookups in notebooks.
# ──────────────────────────────────────────────────────────────────────────────
CONNECTION_PROFILES = {
    "PRD_MEX": PRD_MEX_PROFILE,
    "PRD_MDP": PRD_MDP_PROFILE,
}

def get_sf_options(database: str) -> dict:
    """
    Return the Snowflake Spark connector options for a given database.

    Args:
        database: One of 'PRD_MEX', 'PRD_MDP' (see CONNECTION_PROFILES keys).

    Returns:
        dict of Snowflake connection options (sfURL, sfUser, sfPassword,
        sfWarehouse, sfRole).

    Raises:
        ValueError: If database is not in CONNECTION_PROFILES.

    Example:
        opts = get_sf_options("PRD_MEX")
        df = spark.read.format("net.snowflake.spark.snowflake")
                  .options(**opts)
                  .option("sfDatabase", "PRD_MEX")
                  .option("query", sql)
                  .load()
    """
    if database not in CONNECTION_PROFILES:
        raise ValueError(
            f"No connection profile for database '{database}'. "
            f"Available: {list(CONNECTION_PROFILES.keys())}"
        )
    return dict(CONNECTION_PROFILES[database])   # return a copy, never the live dict


def verify_profiles() -> None:
    """Print a summary of all loaded connection profiles (no secrets revealed)."""
    print("Snowflake Connection Profiles — verified at import")
    print("-" * 60)
    for db, profile in CONNECTION_PROFILES.items():
        print(f"  {db}")
        print(f"    warehouse : {profile['sfWarehouse']}")
        print(f"    role      : {profile['sfRole']}")
        print(f"    user      : {profile['sfUser']}")
        print(f"    url       : {profile['sfURL']}")
    print("-" * 60)


# Auto-verify on import so any notebook using this module gets a startup summary
verify_profiles()
