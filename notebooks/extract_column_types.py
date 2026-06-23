# Databricks notebook source
# MAGIC %md
# MAGIC # Extract Column Types — Snowflake INFORMATION_SCHEMA
# MAGIC
# MAGIC **Purpose**: Queries Snowflake `INFORMATION_SCHEMA.COLUMNS` for every source table
# MAGIC consumed by the semantic layer and generates `configs/column_types_snapshot.yaml`.
# MAGIC
# MAGIC **Credentials**: Uses Databricks secret scope `DAN-AM-P-KVT800-R-MDP-DB`
# MAGIC (Azure Key Vault backed).
# MAGIC
# MAGIC **Usage**: Run all cells top-to-bottom. The output YAML is written to the repo
# MAGIC workspace at `configs/column_types_snapshot.yaml`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

import hashlib
import json
from collections import OrderedDict
from datetime import datetime, timezone

# ═══════════════════════════════════════════════════════════════════════════════
# Snowflake Connection Profiles — PER DATABASE
# ═══════════════════════════════════════════════════════════════════════════════
# Different databases require different credentials/warehouses/roles.
# Each profile maps a database name → its connection parameters.
#
# ⚠️  Move credentials to Databricks secrets for production use:
#     dbutils.secrets.get(scope="<scope>", key="<key>")
# ═══════════════════════════════════════════════════════════════════════════════

import os

SF_URL = "danonenam.east-us-2.azure.snowflakecomputing.com"
KV_SCOPE_MEX = "DAN-AM-P-KVT800-R-MEX-DB"
KV_SCOPE_MDP = "DAN-AM-P-KVT800-R-MDP-DB"

def _secret(scope, key, env_fallback=None):
    try:
        return dbutils.secrets.get(scope=scope, key=key)
    except Exception:
        pass
    val = os.getenv(env_fallback) if env_fallback else None
    if val:
        return val
    raise RuntimeError(f"Cannot resolve '{key}' from scope '{scope}'. Set env var '{env_fallback}'.")

# ─── Profile: PRD_MEX ─────────────────────────────────────────────────────────
PRD_MEX_PROFILE = {
    "sfURL":       SF_URL,
    "sfUser":      _secret(KV_SCOPE_MEX, "snowflake-mex-user",     "SF_MEX_USER"),
    "sfPassword":  _secret(KV_SCOPE_MEX, "snowflake-mex-password", "SF_MEX_PASSWORD"),
    "sfWarehouse": "PRD_MEX_ANL_WH",
    "sfRole":      "PRD_MEX_READER",
}

# ─── Profile: PRD_MDP ─────────────────────────────────────────────────────────
KEYVAULT_SCOPE = KV_SCOPE_MDP
PRD_MDP_PROFILE = {
    "sfURL":       SF_URL,
    "sfUser":      _secret(KV_SCOPE_MDP, "snowflake-user",     "SF_MDP_USER"),
    "sfPassword":  _secret(KV_SCOPE_MDP, "snowflake-password", "SF_MDP_PASSWORD"),
    "sfWarehouse": "PRD_MDP_ANL_WH",
    "sfRole":      "PRD_MDP",
}

# ─── Profile Router ───────────────────────────────────────────────────────────
# Maps each database to the correct connection profile
CONNECTION_PROFILES = {
    "PRD_MEX": PRD_MEX_PROFILE,
    "PRD_MDP": PRD_MDP_PROFILE,
}

def get_sf_options(database: str) -> dict:
    """Return the Snowflake connection options for a given database."""
    if database not in CONNECTION_PROFILES:
        raise ValueError(f"No connection profile for database '{database}'. "
                         f"Available: {list(CONNECTION_PROFILES.keys())}")
    return CONNECTION_PROFILES[database]

# Verify profiles loaded
for db, profile in CONNECTION_PROFILES.items():
    print(f"  {db}: warehouse={profile['sfWarehouse']}, role={profile['sfRole']}, user={profile['sfUser']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Source Tables Registry
# MAGIC
# MAGIC All 33 source tables across 4 database/schema groups.

# COMMAND ----------

SOURCE_TABLES = {
    "PRD_MEX.MEX_DSP_DPH_MKT": [
        "VW_IR_YOG_GEL_MT_NLSN_AGG_DATA_PVT",
        "VW_IR_YOG_GEL_MT_NLSN_MKT_DIM",
        "VW_IR_YOG_GEL_MT_NLSN_PER_DIM",
        "VW_IR_YOG_GEL_MT_NLSN_FACT_REF",
        "VW_IR_YOG_GEL_MT_NLSN_PROD_DIM",
        "VW_IND_AGUA_BNF_RT_NLSN_AGG_DATA_PVT",
        "VW_IND_AGUA_BNF_RT_NLSN_PROD_DIM",
        "VW_IND_AGUA_BNF_RT_NLSN_MKT_DIM",
        "VW_IND_AGUA_BNF_RT_NLSN_PER_DIM",
        "VW_IND_AGUA_BNF_RT_NLSN_FACT_REF",
        "VW_IND_AGUA_BNF_ST_NLSN_AGG_DATA_PVT",
        "VW_IND_AGUA_BNF_ST_NLSN_MKT_DIM",
        "VW_IND_AGUA_BNF_ST_NLSN_PER_DIM",
        "VW_IND_AGUA_BNF_ST_NLSN_FACT_REF",
        "VW_IND_AGUA_BNF_ST_NLSN_PROD_DIM",
        "VW_SUST_LECHE_ST_NLSN_AGG_DATA_PVT",
        "VW_SUST_LECHE_ST_NLSN_PROD_DIM",
        "VW_SUST_LECHE_ST_NLSN_MKT_DIM",
        "VW_SUST_LECHE_ST_NLSN_PER_DIM",
        "VW_SUST_LECHE_ST_NLSN_FACT_REF",
    ],
    "PRD_MEX.MEX_DSP_OTC": [
        "VW_FACT_RNV",
        "V_D_CLIENT",
        "V_D_PERIOD",
        "V_D_ITEM",
        "VW_D_CUSTOMER_DICTONARY",
    ],
    "PRD_MDP.MDP_DSP": [
        "VW_FACT_DANONE_IBP",
        "VW_MKT_ECOMM",
        "VW_FACT_SELL_OUT",
        "V_D_PERIOD",
        "VW_D_STORE_RM",
        "VW_D_PRODUCT_RM",
    ],
    "PRD_MDP.MDP_STG": [
        "FACT_MEDIA_OFF",
        "VW_WASTE",
    ],
}

total_tables = sum(len(v) for v in SOURCE_TABLES.values())
print(f"Total source tables to extract: {total_tables}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Query INFORMATION_SCHEMA.COLUMNS

# COMMAND ----------

INFO_SCHEMA_FIELDS = [
    "TABLE_CATALOG",
    "TABLE_SCHEMA",
    "TABLE_NAME",
    "COLUMN_NAME",
    "ORDINAL_POSITION",
    "DATA_TYPE",
    "IS_NULLABLE",
    "CHARACTER_MAXIMUM_LENGTH",
    "NUMERIC_PRECISION",
    "NUMERIC_SCALE",
]

def build_query(database: str, schema: str, table_names: list) -> str:
    """Build SQL against INFORMATION_SCHEMA.COLUMNS for a set of tables."""
    fields = ", ".join(INFO_SCHEMA_FIELDS)
    in_list = ", ".join(f"'{t}'" for t in sorted(table_names))
    return (
        f"SELECT {fields}\n"
        f"  FROM {database}.INFORMATION_SCHEMA.COLUMNS\n"
        f" WHERE TABLE_SCHEMA = '{schema}'\n"
        f"   AND TABLE_NAME IN ({in_list})\n"
        f" ORDER BY TABLE_NAME, ORDINAL_POSITION"
    )

# Build all queries
queries = []
for db_schema, tables in SOURCE_TABLES.items():
    database, schema = db_schema.split(".")
    sql = build_query(database, schema, tables)
    queries.append((database, schema, db_schema, tables, sql))

print(f"Built {len(queries)} queries for {len(SOURCE_TABLES)} schema groups")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Execute Extraction

# COMMAND ----------

tables_result = {}
total_columns = 0

for database, schema, db_schema, table_names, sql in queries:
    print(f"\n{'='*60}")
    print(f"Querying {db_schema} ({len(table_names)} tables)...")
    print(f"{'='*60}")

    # Get the correct connection profile for this database
    opts = get_sf_options(database)
    print(f"  Using profile: user={opts['sfUser']}, warehouse={opts['sfWarehouse']}, role={opts['sfRole']}")

    # Use Spark Snowflake connector to execute the query
    df = (
        spark.read
        .format("net.snowflake.spark.snowflake")
        .options(**{**opts, "sfDatabase": database, "sfSchema": "INFORMATION_SCHEMA"})
        .option("query", sql)
        .load()
    )

    rows = df.collect()
    print(f"  → Retrieved {len(rows)} column definitions")

    for row in rows:
        fq_table = f"{row['TABLE_CATALOG']}.{row['TABLE_SCHEMA']}.{row['TABLE_NAME']}"
        col_name = row["COLUMN_NAME"]

        if fq_table not in tables_result:
            tables_result[fq_table] = {"columns": {}}

        col_meta = {
            "data_type": row["DATA_TYPE"],
            "is_nullable": row["IS_NULLABLE"],
        }
        if row["CHARACTER_MAXIMUM_LENGTH"] is not None:
            col_meta["character_maximum_length"] = int(row["CHARACTER_MAXIMUM_LENGTH"])
        if row["NUMERIC_PRECISION"] is not None:
            col_meta["numeric_precision"] = int(row["NUMERIC_PRECISION"])
        if row["NUMERIC_SCALE"] is not None:
            col_meta["numeric_scale"] = int(row["NUMERIC_SCALE"])

        tables_result[fq_table]["columns"][col_name] = col_meta
        total_columns += 1

    # Warn about missing tables
    found_tables = {f"{r['TABLE_CATALOG']}.{r['TABLE_SCHEMA']}.{r['TABLE_NAME']}" for r in rows}
    for t in table_names:
        expected = f"{database}.{schema}.{t}"
        if expected not in found_tables:
            print(f"  ⚠️ WARNING: No columns found for {expected}")

# Sort by table name
tables_result = dict(sorted(tables_result.items()))

print(f"\n{'='*60}")
print(f"TOTAL: {total_columns} columns across {len(tables_result)} tables")
print(f"{'='*60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Compute Content Hash & Build Snapshot

# COMMAND ----------

# Deterministic SHA-256 hash of the column metadata
canonical_json = json.dumps(tables_result, sort_keys=True, default=str)
content_hash = f"sha256:{hashlib.sha256(canonical_json.encode('utf-8')).hexdigest()}"

snapshot = {
    "_meta": {
        "snapshot_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "content_hash": content_hash,
        "snowflake_host": SF_URL,
        "connection_profiles": {
            db: {"warehouse": p["sfWarehouse"], "role": p["sfRole"], "user": p["sfUser"]}
            for db, p in CONNECTION_PROFILES.items()
        },
        "extracted_via": "databricks_notebook",
        "notebook_path": dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get(),
    },
    "tables": tables_result,
}

print(f"Content hash: {content_hash}")
print(f"Timestamp:    {snapshot['_meta']['snapshot_timestamp']}")
print(f"Tables:       {len(tables_result)}")
print(f"Columns:      {total_columns}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Write YAML to Repo

# COMMAND ----------

import yaml
import os

# ─── Auto-detect repo root from notebook context ─────────────────────────────
# In Databricks Repos, the notebook runs from within the repo directory tree.
# We detect the repo root dynamically so you never need to hardcode a path.
try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    # notebook_path is like: /Repos/<user>/Market-Driven-.../notebooks/extract_column_types
    # We need to go up from /notebooks/ to the repo root
    # Convert workspace path to filesystem path
    workspace_root = "/Workspace" + notebook_path.rsplit("/notebooks/", 1)[0]
    REPO_ROOT = workspace_root
    print(f"Auto-detected repo root: {REPO_ROOT}")
except Exception:
    # Fallback: manual path
    REPO_ROOT = "/Workspace/Repos/Market Growth Intelligence"
    print(f"⚠️ Could not auto-detect repo root. Using fallback: {REPO_ROOT}")

output_path = os.path.join(REPO_ROOT, "configs", "column_types_snapshot.yaml")
print(f"Output path: {output_path}")

yaml_content = yaml.safe_dump(
    snapshot,
    default_flow_style=False,
    sort_keys=False,
    allow_unicode=True,
    width=120,
)

# Write using Python file I/O (works in Databricks Repos)
with open(output_path, "w", encoding="utf-8") as f:
    f.write(yaml_content)
print(f"✅ Snapshot written to {output_path}")
print(f"   {len(tables_result)} tables, {total_columns} columns")
print(f"   Hash: {content_hash}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verify Output

# COMMAND ----------

# Quick verification — read it back and check structure
with open(output_path, "r", encoding="utf-8") as f:
    content = f.read(2000)
print(content)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Summary
# MAGIC
# MAGIC After running this notebook:
# MAGIC 1. `configs/column_types_snapshot.yaml` is populated with live Snowflake types
# MAGIC 2. Pull the file to your local repo
# MAGIC 3. Re-run the registry builder to replace `PENDING_SNAPSHOT`:
# MAGIC    ```bash
# MAGIC    python scripts/build_registry.py
# MAGIC    ```
# MAGIC 4. Verify with:
# MAGIC    ```bash
# MAGIC    python scripts/build_registry.py --check
# MAGIC    ```

# COMMAND ----------

# Display a summary table for visual inspection
summary_rows = []
for fq_table, info in sorted(tables_result.items()):
    col_count = len(info["columns"])
    col_types = {}
    for col_meta in info["columns"].values():
        dt = col_meta["data_type"]
        col_types[dt] = col_types.get(dt, 0) + 1
    type_summary = ", ".join(f"{k}({v})" for k, v in sorted(col_types.items()))
    summary_rows.append((fq_table, col_count, type_summary))

summary_df = spark.createDataFrame(
    summary_rows,
    ["Table", "Column_Count", "Type_Distribution"]
)
display(summary_df)

# COMMAND ----------


