# Databricks notebook source
# ═══════════════════════════════════════════════════════════════════════════════
# SNOWFLAKE CONNECTION PROFILES — SINGLE SOURCE OF TRUTH
# configs/snowflake_connection_profiles.py
# ═══════════════════════════════════════════════════════════════════════════════
#
# HOW CREDENTIALS ARE RESOLVED (in priority order):
#   1. configs/snowflake_creds.py  — local file, GITIGNORED, never goes to GitHub
#   2. Databricks Key Vault        — dbutils.secrets.get(scope, key)
#   3. Environment variables       — for local dev / CI pipelines
#
# To set up: copy configs/snowflake_creds.example.py → configs/snowflake_creds.py
#            and fill in your credentials. That file will never be committed.
#
# Reference : SEMANTIC_LAYOUTS/INFRASTRUCTURE/SNOWFLAKE_CONNECTION.txt
# Issues log: SEMANTIC_LAYOUTS/INFRASTRUCTURE/CONNECTION_ISSUES.txt
#
# IMPORTANT — role names are NOT symmetric:
#   PRD_MEX → sfRole = "PRD_MEX_READER"  (valid)
#   PRD_MDP → sfRole = "PRD_MDP"         (valid — NOT "PRD_MDP_READER", see ISSUE-002)
#
# Cross-DB constraint: PRD_MEX + PRD_MDP CANNOT share a Snowflake session.
#   Run two separate spark.read calls; join results in Python (see ISSUE-003).
# ═══════════════════════════════════════════════════════════════════════════════

import os
import importlib.util

SF_URL = "danonenam.east-us-2.azure.snowflakecomputing.com"

# ─── Step 1: Try to load local credentials file (gitignored) ──────────────────
_creds = None
try:
    _creds_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "snowflake_creds.py")
    if os.path.exists(_creds_path):
        _spec = importlib.util.spec_from_file_location("snowflake_creds", _creds_path)
        _mod  = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_mod)
        _creds = _mod
        print("  [snowflake_connection_profiles] Loaded local credentials from configs/snowflake_creds.py")
except Exception as _e:
    print(f"  [snowflake_connection_profiles] Could not load snowflake_creds.py: {_e}")


# ─── Step 2: Secret resolver ──────────────────────────────────────────────────
def _secret(local_val, kv_scope, kv_key, env_var=None):
    """
    Resolve a credential in priority order:
      1. local_val  — value from configs/snowflake_creds.py (if file exists and not None)
      2. KV secret  — dbutils.secrets.get(kv_scope, kv_key)
      3. env var    — os.getenv(env_var)
    """
    # Priority 1: local creds file
    if local_val is not None:
        return local_val
    # Priority 2: Databricks Key Vault
    try:
        return dbutils.secrets.get(scope=kv_scope, key=kv_key)   # noqa: F821
    except Exception:
        pass
    # Priority 3: environment variable
    if env_var:
        val = os.getenv(env_var)
        if val:
            return val
    raise RuntimeError(
        f"Cannot resolve credential '{kv_key}'. "
        f"Options: (1) create configs/snowflake_creds.py from the .example file, "
        f"(2) set up KV scope '{kv_scope}', or (3) set env var '{env_var}'."
    )


# ─── Key Vault scopes (used if local creds file is absent) ────────────────────
KV_SCOPE_MEX = "DAN-AM-P-KVT800-R-MEX-DB"
KV_SCOPE_MDP = "DAN-AM-P-KVT800-R-MDP-DB"


# ─── Profile: PRD_MEX ─────────────────────────────────────────────────────────
# Database  : PRD_MEX — Mexico commercial data (Nielsen, MKT, Waste, Water, etc.)
# Role      : PRD_MEX_READER — read-only analyst role
# Warehouse : PRD_MEX_ANL_WH
# Validated : V1–V12 ✅ (2026-06-23)
# ──────────────────────────────────────────────────────────────────────────────
PRD_MEX_PROFILE = {
    "sfURL":       SF_URL,
    "sfUser":      _secret(
                       getattr(_creds, "SF_MEX_USER", None),
                       KV_SCOPE_MEX, "snowflake-mex-user", "SF_MEX_USER"),
    "sfPassword":  _secret(
                       getattr(_creds, "SF_MEX_PASSWORD", None),
                       KV_SCOPE_MEX, "snowflake-mex-password", "SF_MEX_PASSWORD"),
    "sfWarehouse": getattr(_creds, "SF_MEX_WH",   "PRD_MEX_ANL_WH"),
    "sfRole":      getattr(_creds, "SF_MEX_ROLE",  "PRD_MEX_READER"),
}

# ─── Profile: PRD_MDP ─────────────────────────────────────────────────────────
# Database  : PRD_MDP — IBP / demand planning data
# Role      : PRD_MDP  ← NOT "PRD_MDP_READER" (does not exist, see ISSUE-002)
# Warehouse : PRD_MDP_ANL_WH
# sfUser/sfPassword: resolved from KV scope DAN-AM-P-KVT800-R-MDP-DB
# Validated : V12E ✅ (2026-06-23)
# ──────────────────────────────────────────────────────────────────────────────
KEYVAULT_SCOPE = KV_SCOPE_MDP   # kept for backward-compat with existing notebooks
PRD_MDP_PROFILE = {
    "sfURL":       SF_URL,
    "sfUser":      _secret(
                       getattr(_creds, "SF_MDP_USER", None),
                       KV_SCOPE_MDP, "snowflake-user", "SF_MDP_USER"),
    "sfPassword":  _secret(
                       getattr(_creds, "SF_MDP_PASSWORD", None),
                       KV_SCOPE_MDP, "snowflake-password", "SF_MDP_PASSWORD"),
    "sfWarehouse": getattr(_creds, "SF_MDP_WH",   "PRD_MDP_ANL_WH"),
    "sfRole":      getattr(_creds, "SF_MDP_ROLE",  "PRD_MDP"),  # literal — NOT from KV
}


# ─── Profile Router ───────────────────────────────────────────────────────────
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
        dict — a copy of the profile (sfURL, sfUser, sfPassword, sfWarehouse, sfRole).
    Raises:
        ValueError  if database is not registered.
        RuntimeError if credentials cannot be resolved.
    """
    if database not in CONNECTION_PROFILES:
        raise ValueError(
            f"No connection profile for database '{database}'. "
            f"Available: {list(CONNECTION_PROFILES.keys())}"
        )
    return dict(CONNECTION_PROFILES[database])   # always return a copy


# ─── Startup summary (no secrets printed) ─────────────────────────────────────
print("Snowflake Connection Profiles — ready")
print("-" * 50)
for _db, _p in CONNECTION_PROFILES.items():
    print(f"  {_db}: warehouse={_p['sfWarehouse']}  role={_p['sfRole']}")
print("-" * 50)
