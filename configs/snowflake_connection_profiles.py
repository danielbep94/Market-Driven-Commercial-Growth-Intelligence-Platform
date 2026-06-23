# Databricks notebook source
# ═══════════════════════════════════════════════════════════════════════════════
# SNOWFLAKE CONNECTION PROFILES — SINGLE SOURCE OF TRUTH
# configs/snowflake_connection_profiles.py
# ═══════════════════════════════════════════════════════════════════════════════
#
# ⚠️  CREDENTIAL POLICY — READ BEFORE EDITING ⚠️
#
#   ALL credentials (user, password, role, warehouse) must be resolved from
#   Databricks secret scopes (Azure Key Vault). NEVER hardcode passwords,
#   tokens, or service account secrets in this file or any notebook.
#
#   Approved secret managers:
#     - Databricks secrets (dbutils.secrets.get) backed by Azure Key Vault
#     - Environment variables (os.getenv) for local dev / CI pipelines
#
#   Key Vault scopes in use:
#     DAN-AM-P-KVT800-R-MDP-DB  → PRD_MDP credentials
#     DAN-AM-P-KVT800-R-MEX-DB  → PRD_MEX credentials
#       (if MEX scope is not yet provisioned, request DBA to add:
#        snowflake-mex-user, snowflake-mex-password)
#
#   Reference: SEMANTIC_LAYOUTS/INFRASTRUCTURE/SNOWFLAKE_CONNECTION.txt
#   Issues log: SEMANTIC_LAYOUTS/INFRASTRUCTURE/CONNECTION_ISSUES.txt
#
# ═══════════════════════════════════════════════════════════════════════════════
# IMPORTANT — role names are NOT symmetric:
#   PRD_MEX → sfRole = "PRD_MEX_READER"  (valid)
#   PRD_MDP → sfRole = "PRD_MDP"         (valid)
#   PRD_MDP → sfRole = "PRD_MDP_READER"  ← DOES NOT EXIST (see ISSUE-002)
#
# Cross-DB constraint: PRD_MEX and PRD_MDP CANNOT share a Snowflake session.
#   Run two separate spark.read calls; join results in Python (see ISSUE-003).
# ═══════════════════════════════════════════════════════════════════════════════

import os

SF_URL = "danonenam.east-us-2.azure.snowflakecomputing.com"

# ─── Key Vault Scopes ─────────────────────────────────────────────────────────
KV_SCOPE_MEX = "DAN-AM-P-KVT800-R-MEX-DB"   # PRD_MEX credentials
KV_SCOPE_MDP = "DAN-AM-P-KVT800-R-MDP-DB"   # PRD_MDP credentials

# ─── Helper: resolve secrets safely ───────────────────────────────────────────
def _secret(scope: str, key: str, env_fallback: str = None) -> str:
    """
    Resolve a secret from Databricks Key Vault.
    Falls back to an environment variable for local dev / CI.
    Raises a clear error if neither is available.
    """
    # Try Databricks secret scope first (Databricks runtime)
    try:
        return dbutils.secrets.get(scope=scope, key=key)   # noqa: F821
    except Exception:
        pass
    # Fall back to environment variable (local dev / CI)
    if env_fallback:
        val = os.getenv(env_fallback)
        if val:
            return val
    raise RuntimeError(
        f"Could not resolve secret '{key}' from scope '{scope}'. "
        f"On Databricks: ensure Key Vault scope '{scope}' has key '{key}'. "
        f"Locally: set environment variable '{env_fallback}'."
    )


# ─── Profile: PRD_MEX ─────────────────────────────────────────────────────────
# Database  : PRD_MEX — Mexico commercial data (Nielsen, MKT, Waste, Water, etc.)
# Role      : PRD_MEX_READER   — read-only analyst role
# Warehouse : PRD_MEX_ANL_WH
# Validated : V1–V12 ✅ (2026-06-23)
#
# ⚠️  If KV scope DAN-AM-P-KVT800-R-MEX-DB is not yet provisioned, request DBA
#     to add keys: snowflake-mex-user, snowflake-mex-password
# ──────────────────────────────────────────────────────────────────────────────
PRD_MEX_PROFILE = {
    "sfURL":       SF_URL,
    "sfUser":      _secret(KV_SCOPE_MEX, "snowflake-mex-user",     "SF_MEX_USER"),
    "sfPassword":  _secret(KV_SCOPE_MEX, "snowflake-mex-password", "SF_MEX_PASSWORD"),
    "sfWarehouse": "PRD_MEX_ANL_WH",
    "sfRole":      "PRD_MEX_READER",
}

# ─── Profile: PRD_MDP ─────────────────────────────────────────────────────────
# Database  : PRD_MDP — MDP / IBP demand planning data
# Role      : PRD_MDP   ← literal string; NOT "PRD_MDP_READER" (does not exist)
# Warehouse : PRD_MDP_ANL_WH
# Credentials: Key Vault scope DAN-AM-P-KVT800-R-MDP-DB
# Validated : V12E ✅ (2026-06-23)
# ──────────────────────────────────────────────────────────────────────────────
PRD_MDP_PROFILE = {
    "sfURL":       SF_URL,
    "sfUser":      _secret(KV_SCOPE_MDP, "snowflake-user",     "SF_MDP_USER"),
    "sfPassword":  _secret(KV_SCOPE_MDP, "snowflake-password", "SF_MDP_PASSWORD"),
    "sfWarehouse": "PRD_MDP_ANL_WH",
    "sfRole":      "PRD_MDP",       # ← literal string, NOT from Key Vault
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
    Return Snowflake Spark connector options for a given database.

    Args:
        database: One of 'PRD_MEX', 'PRD_MDP'.

    Returns:
        dict of sfURL, sfUser, sfPassword, sfWarehouse, sfRole (a copy).

    Raises:
        ValueError: If database is not in CONNECTION_PROFILES.
        RuntimeError: If Key Vault secret cannot be resolved.

    Example:
        opts = get_sf_options("PRD_MEX")
        df = (spark.read
                   .format("net.snowflake.spark.snowflake")
                   .options(**opts)
                   .option("sfDatabase", "PRD_MEX")
                   .option("query", sql)
                   .load())
    """
    if database not in CONNECTION_PROFILES:
        raise ValueError(
            f"No connection profile for database '{database}'. "
            f"Available: {list(CONNECTION_PROFILES.keys())}"
        )
    return dict(CONNECTION_PROFILES[database])   # return a copy, never the live dict


def verify_profiles() -> None:
    """Print a summary of all loaded connection profiles (no secrets revealed)."""
    print("Snowflake Connection Profiles — loaded")
    print("-" * 60)
    for db, profile in CONNECTION_PROFILES.items():
        print(f"  {db}")
        print(f"    warehouse : {profile['sfWarehouse']}")
        print(f"    role      : {profile['sfRole']}")
        print(f"    url       : {profile['sfURL']}")
        print(f"    user      : {'[from KV]' if 'secret' not in str(profile['sfUser']) else profile['sfUser']}")
    print("-" * 60)


# Auto-verify on import
verify_profiles()
