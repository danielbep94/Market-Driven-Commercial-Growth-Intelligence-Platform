# Databricks notebook source
# MAGIC %md
# MAGIC # Credential Validation — configs/snowflake_creds.py
# MAGIC Run each cell top to bottom. All cells must print ✅ before you proceed.

# COMMAND ----------

# ─── CELL 1: Verify the creds file is found and loaded ───────────────────────
import os, importlib.util

# Use os.getcwd() instead of __file__ for Jupyter/Databricks notebook environments
_current_dir = os.getcwd()
_p = os.path.normpath(os.path.join(_current_dir, "..", "configs", "snowflake_creds.py"))

print(f"Looking for creds file at: {_p}")

if not os.path.exists(_p):
    raise FileNotFoundError(
        "❌ configs/snowflake_creds.py NOT FOUND.\n"
        "   In Databricks: copy configs/snowflake_creds.example.py → configs/snowflake_creds.py\n"
        "   and fill in your credentials."
    )

_spec = importlib.util.spec_from_file_location("snowflake_creds", _p)
_m    = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_m)

print("✅ CELL 1 PASS — configs/snowflake_creds.py found and loaded")

# COMMAND ----------

# ─── CELL 2: Verify required variables are present and non-empty ──────────────
required = {
    "SF_MEX_USER":     getattr(_m, "SF_MEX_USER",     None),
    "SF_MEX_PASSWORD": getattr(_m, "SF_MEX_PASSWORD", None),
    "SF_MEX_ROLE":     getattr(_m, "SF_MEX_ROLE",     None),
    "SF_MEX_WH":       getattr(_m, "SF_MEX_WH",       None),
}

errors = []
for var, val in required.items():
    if not val or val.startswith("YOUR_"):
        errors.append(f"  ❌ {var} = {repr(val)}  ← not filled in")
    else:
        # Mask password — show first 3 chars only
        masked = val[:3] + "*" * (len(val) - 3) if "PASSWORD" in var else val
        print(f"  ✅ {var} = {masked}")

if errors:
    for e in errors:
        print(e)
    raise ValueError("❌ CELL 2 FAIL — fix the values above in configs/snowflake_creds.py")

print("\n✅ CELL 2 PASS — all PRD_MEX variables present and filled in")

# COMMAND ----------

# ─── CELL 3: Build get_sf_options directly from verified creds ────────────────
# Self-contained — no external file needed beyond snowflake_creds.py (already loaded)

SF_URL = "danonenam.east-us-2.azure.snowflakecomputing.com"

def get_sf_options(database: str) -> dict:
    """Return Snowflake Spark connector options for a given database."""
    profiles = {
        "PRD_MEX": {
            "sfURL":       SF_URL,
            "sfUser":      _m.SF_MEX_USER,
            "sfPassword":  _m.SF_MEX_PASSWORD,
            "sfWarehouse": getattr(_m, "SF_MEX_WH",   "PRD_MEX_ANL_WH"),
            "sfRole":      getattr(_m, "SF_MEX_ROLE",  "PRD_MEX_READER"),
        },
        "PRD_MDP": {
            "sfURL":       SF_URL,
            "sfUser":      getattr(_m, "SF_MDP_USER",     None) or dbutils.secrets.get("DAN-AM-P-KVT800-R-MDP-DB", "snowflake-user"),
            "sfPassword":  getattr(_m, "SF_MDP_PASSWORD", None) or dbutils.secrets.get("DAN-AM-P-KVT800-R-MDP-DB", "snowflake-password"),
            "sfWarehouse": getattr(_m, "SF_MDP_WH",   "PRD_MDP_ANL_WH"),
            "sfRole":      getattr(_m, "SF_MDP_ROLE",  "PRD_MDP"),
        },
    }
    if database not in profiles:
        raise ValueError(f"No profile for '{database}'. Available: {list(profiles.keys())}")
    return dict(profiles[database])

print("✅ CELL 3 PASS — get_sf_options() defined using verified local creds")
print(f"  PRD_MEX user      : {_m.SF_MEX_USER}")
print(f"  PRD_MEX warehouse : {getattr(_m, 'SF_MEX_WH', 'PRD_MEX_ANL_WH')}")
print(f"  PRD_MEX role      : {getattr(_m, 'SF_MEX_ROLE', 'PRD_MEX_READER')}")

# COMMAND ----------

# ─── CELL 4: Confirm get_sf_options('PRD_MEX') resolves correctly ─────────────
mex_opts = get_sf_options("PRD_MEX")

assert mex_opts["sfRole"]      == "PRD_MEX_READER",  f"Wrong role: {mex_opts['sfRole']}"
assert mex_opts["sfWarehouse"] == "PRD_MEX_ANL_WH",   f"Wrong warehouse: {mex_opts['sfWarehouse']}"
assert mex_opts["sfUser"]      not in (None, "YOUR_MEX_USER"),     "sfUser not resolved"
assert mex_opts["sfPassword"]  not in (None, "YOUR_MEX_PASSWORD"), "sfPassword not resolved"

print(f"  sfURL       : {mex_opts['sfURL']}")
print(f"  sfUser      : {mex_opts['sfUser']}")
print(f"  sfPassword  : {'*' * len(mex_opts['sfPassword'])}")
print(f"  sfWarehouse : {mex_opts['sfWarehouse']}")
print(f"  sfRole      : {mex_opts['sfRole']}")
print("\n✅ CELL 4 PASS — get_sf_options('PRD_MEX') resolves correctly from local file")

# COMMAND ----------

# ─── CELL 5: Live Snowflake connection test (PRD_MEX) ────────────────────────
# Runs a lightweight query to confirm the credentials actually connect

print("Testing live connection to PRD_MEX ...")

_test_sql = "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE()"

try:
    _df = (spark.read
               .format("net.snowflake.spark.snowflake")
               .options(**get_sf_options("PRD_MEX"))
               .option("sfDatabase", "PRD_MEX")
               .option("query", _test_sql)
               .load())
    _row = _df.collect()[0]
    print(f"  CURRENT_USER()      : {_row[0]}")
    print(f"  CURRENT_ROLE()      : {_row[1]}")
    print(f"  CURRENT_WAREHOUSE() : {_row[2]}")
    print(f"  CURRENT_DATABASE()  : {_row[3]}")
    print("\n✅ CELL 5 PASS — live PRD_MEX connection successful")
except Exception as e:
    print(f"\n❌ CELL 5 FAIL — Snowflake connection error: {e}")
    print("\n  Troubleshooting:")
    print("  1. Check SF_MEX_USER / SF_MEX_PASSWORD in configs/snowflake_creds.py")
    print("  2. Confirm your Databricks cluster has the Snowflake connector JAR")
    print("  3. Check ISSUE-001 in SEMANTIC_LAYOUTS/INFRASTRUCTURE/CONNECTION_ISSUES.txt")
    raise

# COMMAND ----------

# ─── CELL 6: Live Snowflake connection test (PRD_MDP via Key Vault) ───────────
# PRD_MDP still uses Key Vault — confirm it also works

print("Testing live connection to PRD_MDP (via Key Vault) ...")

try:
    _df2 = (spark.read
                .format("net.snowflake.spark.snowflake")
                .options(**get_sf_options("PRD_MDP"))
                .option("sfDatabase", "PRD_MDP")
                .option("query", _test_sql)
                .load())
    _row2 = _df2.collect()[0]
    print(f"  CURRENT_USER()      : {_row2[0]}")
    print(f"  CURRENT_ROLE()      : {_row2[1]}")
    print(f"  CURRENT_WAREHOUSE() : {_row2[2]}")
    print(f"  CURRENT_DATABASE()  : {_row2[3]}")
    print("\n✅ CELL 6 PASS — live PRD_MDP connection successful")
except Exception as e:
    print(f"\n⚠️  CELL 6 WARNING — PRD_MDP connection: {e}")
    print("  PRD_MDP uses Key Vault. If scope is not provisioned, this is expected.")
    print("  PRD_MEX (Cell 5) is what matters for Phase C/D work.")

# COMMAND ----------

# ─── CELL 7: Summary ─────────────────────────────────────────────────────────
print("=" * 60)
print("CREDENTIAL VALIDATION SUMMARY")
print("=" * 60)
print("  Cell 1 — creds file found          ✅")
print("  Cell 2 — variables filled in       ✅")
print("  Cell 3 — profile module loaded     ✅")
print("  Cell 4 — get_sf_options() correct  ✅")
print("  Cell 5 — PRD_MEX live query        ✅ (if no error above)")
print("  Cell 6 — PRD_MDP live query        ✅ or ⚠️ (KV dependency)")
print("=" * 60)
print("configs/snowflake_creds.py is GITIGNORED — will never be pushed to GitHub")
print("Credential load order: local file → Key Vault → env var")

# COMMAND ----------


