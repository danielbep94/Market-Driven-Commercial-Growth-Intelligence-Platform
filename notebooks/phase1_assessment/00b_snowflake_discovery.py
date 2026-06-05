# Databricks notebook source
# MAGIC %md
# MAGIC # 00b · Enterprise Source Discovery & Metadata Catalog
# MAGIC
<<<<<<< HEAD
# MAGIC ## Purpose
# MAGIC This notebook replaces the original profiling-only discovery script and transforms it into
# MAGIC a full **Enterprise Metadata Catalog engine**. It runs 9 structured steps per source and
# MAGIC stores all results in a shared `results` dict consumed by `00c_enterprise_catalog_writer.py`.
# MAGIC
# MAGIC ## Steps per Source
# MAGIC | Step | Name | Output |
# MAGIC |------|------|--------|
# MAGIC | 1 | Diagnostic + INFORMATION_SCHEMA | Row count, column count, native SF types |
# MAGIC | 2 | Full Column Inventory | Every column: distinct count, null %, sample values, PK flag |
# MAGIC | 3 | Business Domain Profile | Value frequency tables for all business key columns |
# MAGIC | 4 | DQ Rules Validation | Configurable rules from `configs/dq_rules_catalog.yaml` |
# MAGIC | 5 | Temporal Coverage + Gap Detection | Min/max dates, distinct periods, weekly gap scan |
# MAGIC | 6 | Automatic Grain Detection | Smallest column combo achieving highest uniqueness % |
# MAGIC | 7 | Numeric Volume + Anomaly Flags | Min/max/sum/negatives for all numeric columns |
# MAGIC | 8 | Duplicate / PK Analysis | Row-level duplicate check on candidate grain |
# MAGIC | 9 | Enterprise Readiness Score | 6-dimension scoring replacing old 4-dim score |
# MAGIC
# MAGIC ## SQL Syntax Rule
# MAGIC The Snowflake Spark connector does **NOT** support `DATABASE.SCHEMA.TABLE` inside SQL.
# MAGIC - ✅ `SELECT * FROM VW_MKT_ECOMM WHERE anio >= 2024`
# MAGIC - ✅ `SELECT * FROM MDP_DSP.VW_MKT_ECOMM` ← cross-schema JOIN only
# MAGIC - ❌ `SELECT * FROM PRD_MDP.MDP_DSP.VW_MKT_ECOMM`
# MAGIC
# MAGIC The `db` key goes in connector options. SQL uses `SCHEMA.TABLE` or just `TABLE`.

# COMMAND ----------
# MAGIC %md ## ─── SECTION A: SOURCES CONFIGURATION ──────────────────────────────
=======
# MAGIC ## Rule for SQL definitions
# MAGIC Snowflake object resolution depends on the active Snowflake user, role, database, schema, and warehouse.
# MAGIC - ✅ `SELECT * FROM VW_MKT_ECOMM WHERE anio >= 2024` — uses the connector `db` + `schema` context
# MAGIC - ✅ `SELECT * FROM MDP_DSP.VW_MKT_ECOMM WHERE anio >= 2024` — explicitly sets the schema
# MAGIC - ✅ `SELECT * FROM PRD_MDP.MDP_DSP.VW_MKT_ECOMM WHERE anio >= 2024` — fully qualifies the object
# MAGIC
# MAGIC If a query fails with `does not exist or not authorized`, validate the active Snowflake context and object grants before assuming the object name format is wrong.
# MAGIC If data discovery reports a query-related read issue while using role `PRD_MDP`, the likely cause is that the underlying table/view was created under a different user and needs to be recreated manually so the connector role can read it. Do not switch to `PRD_MDP_READER`; it uses a different permission model.

# COMMAND ----------

# MAGIC %md ## ─── EDIT THIS SECTION ──────────────────────────────────────────
>>>>>>> f4a62afb7c099871a80062d0d10ad5eb4194f254
# MAGIC
# MAGIC Configure each source below. Set `None` for sources not yet ready.
# MAGIC
# MAGIC Keys per source dict:
# MAGIC - `"db"`:     Snowflake database (e.g. `"PRD_MDP"`)
<<<<<<< HEAD
# MAGIC - `"schema"`: Default schema (e.g. `"MDP_DSP"`)
# MAGIC - `"sql"`:    Query — use `TABLE` or `SCHEMA.TABLE`, never `DB.SCHEMA.TABLE`
# MAGIC - `"grain_hint"`: Optional human description to be validated (e.g. `"FECHA × MARCA × CAMPANA"`)
=======
# MAGIC - `"schema"`: Default schema for this source (e.g. `"MDP_DSP"`)
# MAGIC - `"sql"`:    Query — use `TABLE`, `SCHEMA.TABLE`, or `DB.SCHEMA.TABLE` as needed for the active Snowflake context
# MAGIC
# MAGIC Set value to `None` for sources not yet ready.
>>>>>>> f4a62afb7c099871a80062d0d10ad5eb4194f254

# COMMAND ----------

SOURCES = {

    # ── 1 · Investment / Marketing ──────────────────────────────────────────
    "DATA_MKT": {
<<<<<<< HEAD
        "db":         "PRD_MDP",
        "schema":     "MDP_DSP",
        "sql":        "SELECT * FROM VW_MKT_ECOMM WHERE anio >= 2024",
        "grain_hint": "FECHA × MARCA × CAMPANA × MEDIO",
=======
        "db":     "PRD_MDP",
        "schema": "MDP_DSP",
        "sql":    "SELECT * FROM PRD_MDP.MDP_DSP.VW_MKT_ECOMM WHERE anio >= 2024",
>>>>>>> f4a62afb7c099871a80062d0d10ad5eb4194f254
    },

    # ── 2 · Sell-In ──────────────────────────────────────────────────────────
    "DATA_SELL_IN": None,   # ← replace None with dict when ready

    # ── 3 · Sell-Out ─────────────────────────────────────────────────────────
<<<<<<< HEAD
=======
    "DATA_SELL_OUT": {
        "db":     "PRD_MDP",
        "schema": "MDP_DSP",
        "sql":    """
          WITH fact_filtered AS (
    SELECT
        PER_ID,
        STORE,
        UPC,
        VOL_SELL_OUT,
        PCS_SELL_OUT,
        AMOUNT_SELL_OUT,
        VOL_INV,
        PCS_INV,
        AVG_SELL
    FROM PRD_MDP.MDP_DSP.VW_FACT_SELL_OUT
    WHERE PER_ID >= 20250101
)

SELECT
    -- Period Catalog
    per.DAY_ID,
    per.YEAR_ID,
    per.MONTH_LONG_DES_ESP,

    -- CBU Catalog
    cbu.CBU_CODE,
    cbu.CBU_DSC,
    cbu.CBU_SAP,

    -- Store Catalog
    st.CHAIN,
    st.FORMAT,
    st.SUBCHAIN,

    -- Product Catalog (pon aquí solo las columnas necesarias)
    prod.INT_ID,
    prod.CBU_ID,
    
    -- Fact Metrics
    f.UPC,
    f.VOL_SELL_OUT,
    f.PCS_SELL_OUT,
    f.AMOUNT_SELL_OUT,
    f.VOL_INV,
    f.PCS_INV,
    f.AVG_SELL
FROM fact_filtered f
INNER JOIN PRD_MDP.MDP_DWH.V_D_PERIOD per
    ON f.PER_ID = per.PER_ID
INNER JOIN PRD_MDP.MDP_DSP.VW_D_STORE_RM st
    ON f.STORE = st.INT_ID
INNER JOIN PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM prod
    ON f.UPC = prod.INT_ID
INNER JOIN PRD_MDP.MDP_DWH.VW_CBU_RM cbu
    ON prod.CBU_ID = cbu.CBU_ID;
        """,
    },

>>>>>>> f4a62afb7c099871a80062d0d10ad5eb4194f254
    "DATA_SELL_OUT": None,
    # Example when ready:
    # "DATA_SELL_OUT": {
    #     "db":         "PRD_MDP",
    #     "schema":     "MDP_DSP",
    #     "sql":        "SELECT * FROM VW_FACT_SELL_OUT WHERE anio >= 2024",
    #     "grain_hint": "DAY_ID × UPC × CHAIN",
    # },

    # ── 4 · Waste / Merma ────────────────────────────────────────────────────
    "DATA_WASTE": {
        "db":     "PRD_MDP",
        "schema": "MDP_DSP",
        "sql":    "SELECT * FROM PRD_MDP.MDP_STG.VW_WASTE",
    },

    # ── 5 · Demand Forecast ──────────────────────────────────────────────────
    "DATA_FORECAST": None,

    # ── 6 · Nielsen / Market Share ───────────────────────────────────────────
    "DATA_NIELSEN": None,

    # ── 7 · Price ────────────────────────────────────────────────────────────
    "DATA_PRICE": None,

    # ── 8 · Promotions ───────────────────────────────────────────────────────
    "DATA_PROMO": None,

    # ── 9 · Inventory / Stock ────────────────────────────────────────────────
    "DATA_INVENTORY": None,

    # ── 10 · Calendar / Date Dimension ──────────────────────────────────────
    "DATA_CALENDAR": None,
}

DOMAIN_LABELS = {
    "DATA_MKT":       "Investment / Marketing",
    "DATA_SELL_IN":   "Sell-In",
    "DATA_SELL_OUT":  "Sell-Out",
    "DATA_WASTE":     "Waste / Merma",
    "DATA_FORECAST":  "Demand Forecast",
    "DATA_NIELSEN":   "Nielsen / Market Share",
    "DATA_PRICE":     "Price",
    "DATA_PROMO":     "Promotions",
    "DATA_INVENTORY": "Inventory / Stock",
    "DATA_CALENDAR":  "Calendar / Date Dimension",
}

# COMMAND ----------
<<<<<<< HEAD
# MAGIC %md ## ─── SECTION B: DO NOT EDIT BELOW ───────────────────────────────────

# COMMAND ----------
# MAGIC %md ## B1 · Imports & Connection Setup

# COMMAND ----------
import yaml, csv, os, itertools
from datetime import datetime
from collections import defaultdict

import pyspark.sql.functions as F
from pyspark.sql.types import NumericType, StringType, DateType, TimestampType
from pyspark.sql.window import Window

# ── Credentials ───────────────────────────────────────────────────────────────
=======

# MAGIC %md ## ─── DO NOT EDIT BELOW ───────────────────────────────────────────

# COMMAND ----------

# MAGIC %md ## Connection

# COMMAND ----------

>>>>>>> f4a62afb7c099871a80062d0d10ad5eb4194f254
KEYVAULT_NAME = "DAN-AM-P-KVT800-R-MDP-DB"
KEY_NAME_USR  = "snowflake-user"
KEY_NAME_PWD  = "snowflake-password"
SF_URL        = "danonenam.east-us-2.azure.snowflakecomputing.com"
SF_WAREHOUSE  = "PRD_MDP_ANL_WH"
SF_ROLE       = "PRD_MDP"

try:
    user     = dbutils.secrets.get(scope=KEYVAULT_NAME, key=KEY_NAME_USR)
    password = dbutils.secrets.get(scope=KEYVAULT_NAME, key=KEY_NAME_PWD)
    print(f"✅ Credentials loaded from: {KEYVAULT_NAME}")
except NameError:
    user, password = "MOCK_USER", "MOCK_PASSWORD"
    print("🚨 Non-Databricks env — using MOCK credentials")
except Exception as e:
    user, password = "MOCK_USER", "MOCK_PASSWORD"
    print(f"🚨 Secret error: {e}")

<<<<<<< HEAD
=======
from datetime import datetime
import os
import re
from collections import Counter
import pyspark.sql.functions as F
from pyspark.sql.types import NumericType

>>>>>>> f4a62afb7c099871a80062d0d10ad5eb4194f254
RUN_AT      = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
OUTPUT_DIR  = "docs/phase_outputs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

<<<<<<< HEAD
# ── Snowflake helpers ─────────────────────────────────────────────────────────
def get_sf_opts(db: str, schema: str) -> dict:
    return {
        "sfURL": SF_URL, "sfUser": user, "sfPassword": password,
        "sfDatabase": db, "sfSchema": schema, "sfWarehouse": SF_WAREHOUSE,
=======
def qcol(name: str):
    """Safely reference a Spark DataFrame column by its exact name.

    Spark treats dots in bare strings as nested-field navigation. Wrapping the
    escaped name in backticks forces exact column resolution for names containing
    dots, percent signs, parentheses, spaces, quotes/backticks, or mixed casing.
    """
    return F.col(f"`{str(name).replace('`', '``')}`")

def agg_alias(prefix: str, idx: int) -> str:
    """Create simple aggregate aliases that cannot be confused with source names."""
    return f"__{prefix}_{idx}"

def sql_literal(value) -> str:
    """Safely quote a scalar value for Snowflake diagnostic SQL."""
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"

def quote_snowflake_ident(identifier: str) -> str:
    """Safely quote one Snowflake identifier part for diagnostics."""
    return '"' + str(identifier).replace('"', '""') + '"'

def quote_snowflake_object(*parts: str) -> str:
    """Safely quote a Snowflake object path such as DB.SCHEMA.OBJECT."""
    return ".".join(quote_snowflake_ident(part) for part in parts if part)

def get_sf_opts(db: str, schema: str) -> dict:
    """Build Snowflake connector options for a specific db + schema."""
    missing = [name for name, value in {"db": db, "schema": schema}.items() if not value]
    if missing:
        raise ValueError(f"Missing required Snowflake config key(s): {', '.join(missing)}")
    return {
        "sfURL":       SF_URL,
        "sfUser":      user,
        "sfPassword":  password,
        "sfDatabase":  db,
        "sfSchema":    schema,
        "sfWarehouse": SF_WAREHOUSE,
        "sfRole":      SF_ROLE,
>>>>>>> f4a62afb7c099871a80062d0d10ad5eb4194f254
    }

def validate_source_config(source_key: str, cfg: dict):
    """Fail fast on incomplete source definitions instead of surfacing opaque connector errors."""
    if not isinstance(cfg, dict):
        raise ValueError(f"{source_key}: source config must be a dict or None")
    missing = [key for key in ("db", "schema", "sql") if not cfg.get(key)]
    if missing:
        raise ValueError(f"{source_key}: missing required config key(s): {', '.join(missing)}")

def run_sql(db: str, schema: str, sql: str):
<<<<<<< HEAD
=======
    """
    Execute SQL against Snowflake using the requested database/schema/role context.
    SQL may use TABLE, SCHEMA.TABLE, or DB.SCHEMA.TABLE depending on
    how Snowflake resolves the object for the active user and role.
    """
    if not sql or not str(sql).strip():
        raise ValueError("SQL query is empty")
>>>>>>> f4a62afb7c099871a80062d0d10ad5eb4194f254
    return spark.read.format("snowflake") \
               .options(**get_sf_opts(db, schema)) \
               .option("query", str(sql).strip()) \
               .load()

def run_info_schema(db: str, schema: str, object_name: str):
    """Pull native Snowflake types from INFORMATION_SCHEMA.COLUMNS."""
    try:
        info_sql = f"""
            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH,
                   NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE, ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = UPPER('{object_name}')
            ORDER BY ORDINAL_POSITION
        """
        df = run_sql(db, schema, info_sql)
        return {row["COLUMN_NAME"]: row.asDict() for row in df.collect()}
    except Exception as e:
        print(f"  ⚠️  INFORMATION_SCHEMA query failed: {str(e)[:150]}")
        return {}

# COMMAND ----------
# MAGIC %md ## B2 · Load Config Files

# COMMAND ----------
# ── DQ Rules ─────────────────────────────────────────────────────────────────
DQ_RULES = {}
try:
    with open("configs/dq_rules_catalog.yaml", "r") as f:
        DQ_RULES = yaml.safe_load(f) or {}
    print(f"✅ DQ rules loaded — {len(DQ_RULES)} datasets configured")
except Exception as e:
    print(f"⚠️  Could not load dq_rules_catalog.yaml: {e}")

# ── Business Glossary Seed ────────────────────────────────────────────────────
GLOSSARY_SEED = {}
try:
    with open("configs/business_glossary_seed.yaml", "r") as f:
        seed_data = yaml.safe_load(f) or {}
    for entry in seed_data.get("columns", []):
        GLOSSARY_SEED[entry["column_name"].upper()] = entry
    print(f"✅ Glossary seed loaded — {len(GLOSSARY_SEED)} column definitions")
except Exception as e:
    print(f"⚠️  Could not load business_glossary_seed.yaml: {e}")

# COMMAND ----------
# MAGIC %md ## B3 · Business Key Taxonomy (for Domain Profile & Dimension Discovery)

# COMMAND ----------
# Columns to profile with value-frequency tables (EA1)
BUSINESS_KEY_COLS = {
    "marca", "cadena", "chain", "canal", "sku", "upc", "format", "formato",
    "categoria", "subcategoria", "campana", "medio", "plataforma", "objetivo",
    "subchain", "int_id", "cbu_code", "brand", "category", "subcategory",
}

# Dimension taxonomy (EA4)
DIM_TAXONOMY = {
    "DIM_PRODUCT":   {"upc", "sku", "int_id", "cbu_code", "descripcion", "marca",
                      "product_id", "cod_producto", "ean", "gtin"},
    "DIM_CUSTOMER":  {"cadena", "chain", "subchain", "format", "canal", "customer_id",
                      "cliente", "retailer", "store_id", "tienda"},
    "DIM_DATE":      {"fecha", "fecha_proceso", "fecha_venta", "fecha_cierre",
                      "day_id", "anio", "mes", "year_id", "dt", "date", "week",
                      "semana", "periodo", "period", "year", "month"},
    "DIM_BRAND":     {"marca", "brand", "categoria", "subcategoria", "category",
                      "subcategory", "negocio", "business"},
    "DIM_MARKETING": {"campana", "campaign", "medio", "media", "plataforma",
                      "platform", "objetivo", "objective", "formato"},
}

def classify_dimension(col_lower: str) -> str:
    for dim, keywords in DIM_TAXONOMY.items():
        if col_lower in keywords:
            return dim
    return "FACT / MEASURE"

# Date column detection priority list
DATE_PRIORITY = ["anio", "año", "year", "yr", "periodo", "period",
                 "fecha", "fecha_proceso", "fecha_venta", "fecha_cierre",
                 "date", "dt", "week", "semana", "mes", "month", "day_id"]

def detect_date_col(columns: list):
<<<<<<< HEAD
    col_lower = {c.lower(): c for c in columns}
    for p in DATE_PRIORITY:
=======
    priority = ["anio", "año", "year", "yr", "periodo", "period",
                "fecha", "fecha_proceso", "fecha_venta", "fecha_cierre",
                "date", "dt", "week", "semana", "mes", "month"]
    col_lower = {c.strip().lower(): c for c in columns if isinstance(c, str)}
    for p in priority:
>>>>>>> f4a62afb7c099871a80062d0d10ad5eb4194f254
        if p in col_lower:
            return col_lower[p]
    return None

<<<<<<< HEAD
# COMMAND ----------
# MAGIC %md ## B4 · Main Discovery Loop
=======
def readiness_score(null_results, has_date, has_numeric, dup_pct) -> int:
    high  = sum(1 for n in null_results if "HIGH" in n["flag"])
    warn  = sum(1 for n in null_results if "WARN" in n["flag"])
    comp  = max(0, 25 - high * 5 - warn * 2)
    temp  = 25 if has_date  else 5
    vol   = 25 if has_numeric else 15
    card  = max(0, 25 - min(25, int(dup_pct / 2)))
    return comp + temp + vol + card

def score_label(s):
    return "🟢 READY" if s >= 80 else ("🟡 CONDITIONAL" if s >= 60 else "🔴 NOT READY")

def analyze_column_names(columns: list) -> dict:
    """Detect column-name risks that commonly break dynamic Spark references."""
    counts = Counter(columns)
    lower_counts = Counter(str(c).lower() for c in columns)
    special_pattern = re.compile(r"[^A-Za-z0-9_]")
    alnum_pattern = re.compile(r"[A-Za-z0-9]")
    return {
        "duplicate_columns": [c for c, n in counts.items() if n > 1],
        "case_insensitive_duplicates": [c for c, n in lower_counts.items() if n > 1],
        "leading_trailing_space_columns": [c for c in columns if str(c) != str(c).strip()],
        "special_character_columns": [c for c in columns if special_pattern.search(str(c))],
        "punctuation_only_columns": [c for c in columns if not alnum_pattern.search(str(c))],
    }

# COMMAND ----------

# MAGIC %md ## Validate All Defined Sources
>>>>>>> f4a62afb7c099871a80062d0d10ad5eb4194f254

# COMMAND ----------

defined   = {k: v for k, v in SOURCES.items() if v is not None}
undefined = {k: v for k, v in SOURCES.items() if v is None}

print(f"Sources defined:  {len(defined)}/10")
print(f"Sources pending:  {len(undefined)}/10")
if undefined:
    print(f"  Pending: {', '.join(undefined.keys())}")

<<<<<<< HEAD
results = {}  # Shared across this notebook and 00c
=======
# COMMAND ----------

# MAGIC %md ## Snowflake Context Diagnostic
# MAGIC
# MAGIC Temporary diagnostic cell: run before the validation loop to confirm the active Snowflake user, role, database, schema, warehouse, and whether each configured view is visible to the connector context.

# COMMAND ----------

def _strip_sql_comments(sql: str) -> str:
    """Remove basic SQL comments before lightweight diagnostic parsing."""
    sql = re.sub(r"/\*.*?\*/", " ", str(sql), flags=re.S)
    sql = re.sub(r"--.*?(?=\n|$)", " ", sql)
    return sql

def _split_identifier_parts(identifier: str) -> list:
    """Split DB.SCHEMA.OBJECT while respecting quoted identifier dots."""
    parts, current, quote = [], [], None
    for ch in str(identifier).strip():
        if quote:
            current.append(ch)
            if ch == quote:
                quote = None
            continue
        if ch in ('"', '`'):
            quote = ch
            current.append(ch)
        elif ch == ".":
            parts.append("".join(current).strip().strip('`"'))
            current = []
        else:
            current.append(ch)
    if current:
        parts.append("".join(current).strip().strip('`"'))
    return [p for p in parts if p]

def _tokenize_sql(sql: str) -> list:
    """Tokenize enough SQL for FROM/JOIN diagnostics without executing parsing logic."""
    tokens, current, quote = [], [], None
    for ch in _strip_sql_comments(sql):
        if quote:
            current.append(ch)
            if ch == quote:
                quote = None
            continue
        if ch in ('"', '`', "'"):
            quote = ch
            current.append(ch)
        elif ch.isspace() or ch in ",;()":
            if current:
                tokens.append("".join(current))
                current = []
            if ch in "(),":
                tokens.append(ch)
        else:
            current.append(ch)
    if current:
        tokens.append("".join(current))
    return tokens

def _cte_names(tokens: list) -> set:
    """Best-effort CTE name extraction for queries starting with WITH."""
    names = set()
    if not tokens or tokens[0].lower() != "with":
        return names
    depth = 0
    expect_name = True
    for token in tokens[1:]:
        low = token.lower()
        if token == "(":
            depth += 1
        elif token == ")":
            depth = max(0, depth - 1)
        elif depth == 0 and low == "select":
            break
        elif depth == 0 and expect_name and low not in {"recursive", ","}:
            names.add(token.strip('`"'))
            expect_name = False
        elif depth == 0 and low == "as":
            expect_name = False
        elif depth == 0 and token == ",":
            expect_name = True
    return names

def _extract_source_objects(sql: str) -> list:
    """Best-effort extraction of physical objects after FROM/JOIN for diagnostics.

    This intentionally avoids subqueries and CTE aliases. It is diagnostic-only;
    complex SQL should still be validated by Snowflake execution.
    """
    tokens = _tokenize_sql(sql)
    ctes = _cte_names(tokens)
    refs = []
    for idx, token in enumerate(tokens[:-1]):
        if token.lower() not in {"from", "join"}:
            continue
        candidate = tokens[idx + 1]
        if candidate == "(":
            continue  # subquery; skip because the next token is not a physical object
        candidate = candidate.strip('`";,()')
        if not candidate or candidate.lower() in {"select", "lateral", "table"}:
            continue
        if candidate.strip('`"') in ctes:
            continue
        refs.append(candidate)
    return refs

def _extract_source_object(sql: str):
    """Backward-compatible helper returning the first detected physical object."""
    objects = _extract_source_objects(sql)
    return objects[0] if objects else None

def _split_object_name(db: str, schema: str, object_name: str):
    parts = _split_identifier_parts(object_name)
    if len(parts) >= 3:
        return parts[-3], parts[-2], parts[-1]
    if len(parts) == 2:
        return db, parts[0], parts[1]
    if len(parts) == 1:
        return db, schema, parts[0]
    return db, schema, None

def run_context_diagnostic(source_key: str, cfg: dict):
    """Print Snowflake context and object visibility for a configured source."""
    validate_source_config(source_key, cfg)
    db, schema, sql = cfg["db"], cfg["schema"], cfg["sql"]
    source_objects = _extract_source_objects(sql)
    if not source_objects:
        print(f"  ⚠️  {source_key}: Could not detect a physical FROM/JOIN object for diagnostics")
        return None

    diag_dfs = []
    for source_object in source_objects:
        obj_db, obj_schema, obj_name = _split_object_name(db, schema, source_object)
        if not obj_name:
            print(f"  ⚠️  {source_key}: Could not split object name: {source_object}")
            continue
        info_schema = quote_snowflake_object(obj_db, "INFORMATION_SCHEMA", "TABLES")
        diag_sql = f"""
            SELECT
                CURRENT_USER() AS current_user,
                CURRENT_ROLE() AS current_role,
                CURRENT_DATABASE() AS current_database,
                CURRENT_SCHEMA() AS current_schema,
                CURRENT_WAREHOUSE() AS current_warehouse,
                {sql_literal(obj_db)} AS requested_database,
                {sql_literal(obj_schema)} AS requested_schema,
                {sql_literal(obj_name)} AS requested_object,
                CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM {info_schema}
                        WHERE TABLE_SCHEMA = UPPER({sql_literal(obj_schema)})
                          AND TABLE_NAME = UPPER({sql_literal(obj_name)})
                    ) THEN 'FOUND_TABLE_OR_VIEW'
                    ELSE 'NOT_FOUND_OR_NOT_AUTHORIZED'
                END AS object_visibility
        """
        print(f"\nDiagnostic — {source_key}: {source_object}")
        diag_df = run_sql(obj_db, obj_schema, diag_sql)
        diag_df.show(truncate=False)
        diag_dfs.append(diag_df)
    return diag_dfs[0] if len(diag_dfs) == 1 else diag_dfs

for source_key, cfg in defined.items():
    try:
        run_context_diagnostic(source_key, cfg)
    except Exception as e:
        print(f"\nDiagnostic — {source_key}: FAILED")
        print(f"  {str(e)[:500]}")

results = {}
>>>>>>> f4a62afb7c099871a80062d0d10ad5eb4194f254

for source_key, cfg in defined.items():
    label = DOMAIN_LABELS.get(source_key, source_key)
    db, schema, sql = cfg["db"], cfg["schema"], cfg["sql"]
    grain_hint = cfg.get("grain_hint", "")

    print("\n" + "═" * 72)
    print(f"  {source_key}  —  {label}")
    print("═" * 72)
    print(f"  db={db}  schema={schema}")
    print(f"  SQL: {sql.strip()[:100]}{'...' if len(sql.strip()) > 100 else ''}\n")

    res = {
        # Identity
        "key": source_key, "label": label, "db": db, "schema": schema, "sql": sql,
        "grain_hint": grain_hint, "status": "OK",
        # Counts
        "total_rows": 0, "total_cols": 0,
        # Step 1
        "sf_types": {},
        # Step 2 — Full column inventory
        "col_inventory": [],
        # Step 3 — Business domain profile
        "domain_profile": [],
        # Step 4 — DQ rules
        "dq_results": [],
        "dq_pass_count": 0, "dq_fail_count": 0,
        # Step 5 — Temporal
        "date_col": None, "date_min": None, "date_max": None,
        "date_distinct": None, "temporal_gaps": 0,
        # Step 6 — Grain
        "grain_candidates": [],
        # Step 7 — Numeric
        "numeric_stats": [],
        # Step 8 — Duplicates
        "dup_count": 0, "dup_pct": 0.0, "dup_key_used": [],
        # Step 9 — Enterprise score
        "score_completeness": 0, "score_consistency": 0, "score_joinability": 0,
        "score_temporal": 0, "score_documentation": 0, "score_grain": 0,
        "enterprise_score": 0,
        # Misc
        "errors": [],
    }

    # ── Step 1: Execute + INFORMATION_SCHEMA ─────────────────────────────────
    try:
        validate_source_config(source_key, cfg)
        df = run_sql(db, schema, sql).cache()
        res["total_rows"] = df.count()
        res["total_cols"] = len(df.columns)
        print(f"  ✅ Step 1 — OK  |  Rows: {res['total_rows']:,}  |  Cols: {res['total_cols']}")
    except Exception as e:
        err_msg = str(e)
        res["status"] = "ERROR"
        res["errors"].append(err_msg[:300])
<<<<<<< HEAD
        print(f"  ❌ Step 1 — FAILED: {err_msg[:300]}")
=======
        print(f"  ❌ Step 1 — FAILED")
        print(f"     {err_msg[:300]}")
        if "does not exist or not authorized" in err_msg:
            print(f"\n  💡 Tip: Validate the active Snowflake context and grants.")
            print(f"     Current connector role: {SF_ROLE} | user: {user} | db: {db} | schema: {schema}")
            print(f"     Confirm the view exists and this role is authorized to access it.")
            print(f"     Try the object format that matches your context:")
            print(f"     ✅ FROM VW_MKT_ECOMM")
            print(f"     ✅ FROM MDP_DSP.VW_MKT_ECOMM")
            print(f"     ✅ FROM PRD_MDP.MDP_DSP.VW_MKT_ECOMM")
>>>>>>> f4a62afb7c099871a80062d0d10ad5eb4194f254
        results[source_key] = res
        continue

    # Attempt INFORMATION_SCHEMA pull for native types
    view_name = sql.strip().split("FROM")[-1].strip().split()[0].split(".")[-1]
    res["sf_types"] = run_info_schema(db, schema, view_name)
    if res["sf_types"]:
        print(f"  ✅ INFORMATION_SCHEMA — {len(res['sf_types'])} native type definitions loaded")
    else:
        print(f"  ⚠️  Native Snowflake types not available — using PySpark types only")

    # ── Step 2: Full Column Inventory ────────────────────────────────────────
    print(f"\n  Step 2 — Full Column Inventory ({res['total_cols']} columns)")
    print(f"  {'Column':<45} {'SF Type':<20} {'Nullable':<10} {'Distinct':>10} {'Null %':>8} {'PK?':>6}")
    print("  " + "─" * 100)

    for field in df.schema.fields:
        col = field.name
        col_lower = col.lower()

        # Null analysis
        null_count   = df.filter(F.col(col).isNull()).count()
        null_pct     = round(null_count / res["total_rows"] * 100, 2) if res["total_rows"] > 0 else 0.0

        # Distinct count (every column — not keyword filtered)
        distinct_cnt = df.select(col).distinct().count()

        # PK candidate: unique + never null
        is_pk_cand   = (distinct_cnt == res["total_rows"] and null_pct == 0.0)

        # Sample values: top 5 by frequency (capped at 200 chars)
        try:
            top_vals = (
                df.groupBy(col).count()
                  .orderBy(F.desc("count"))
                  .limit(5)
                  .select(col)
                  .rdd.flatMap(lambda x: x)
                  .collect()
            )
            sample_str = " | ".join([str(v) for v in top_vals if v is not None])[:200]
        except Exception:
            sample_str = ""

        # Native Snowflake type (falls back to PySpark if INFORMATION_SCHEMA unavailable)
        sf_info   = res["sf_types"].get(col.upper(), {})
        sf_type   = sf_info.get("DATA_TYPE", str(field.dataType))

        # Dimension classification
        dim_class = classify_dimension(col_lower)

        # Glossary linkage
        in_glossary = col.upper() in GLOSSARY_SEED

        null_flag = "⚠️ HIGH" if null_pct > 5 else ("🔶 WARN" if null_pct > 1 else "✅ OK")

        print(f"  {col:<45} {sf_type:<20} {'NULL' if field.nullable else 'NOT NULL':<10} "
              f"{distinct_cnt:>10,} {null_pct:>7.2f}% {'✓ PK' if is_pk_cand else '':>6}")

        res["col_inventory"].append({
            "dataset":        source_key,
            "column":         col,
            "pyspark_type":   str(field.dataType),
            "sf_type":        sf_type,
            "nullable":       field.nullable,
            "null_count":     null_count,
            "null_pct":       null_pct,
            "null_flag":      null_flag,
            "distinct_count": distinct_cnt,
            "is_pk_candidate": is_pk_cand,
            "sample_values":  sample_str,
            "dimension_class": dim_class,
            "in_glossary":    in_glossary,
        })

<<<<<<< HEAD
    # ── Step 3: Business Domain Profile ──────────────────────────────────────
    print(f"\n  Step 3 — Business Domain Profile")
    bus_key_found = [c for c in df.columns if c.lower() in BUSINESS_KEY_COLS]
    print(f"  Business key columns found: {bus_key_found or 'none'}")
=======
    column_diagnostics = analyze_column_names(df.columns)
    if column_diagnostics["duplicate_columns"]:
        msg = f"Duplicate column names detected; profiling by name would be ambiguous: {column_diagnostics['duplicate_columns']}"
        res["status"] = "ERROR"
        res["errors"].append(msg)
        print(f"  ❌ {msg}")
        results[source_key] = res
        df.unpersist()
        continue
    if column_diagnostics["case_insensitive_duplicates"]:
        print(f"  ⚠️  Case-insensitive duplicate column names may be ambiguous in case-insensitive Spark sessions: {column_diagnostics['case_insensitive_duplicates']}")
    if column_diagnostics["leading_trailing_space_columns"]:
        print(f"  ⚠️  Columns with leading/trailing spaces: {column_diagnostics['leading_trailing_space_columns']}")
    if column_diagnostics["punctuation_only_columns"]:
        print(f"  ⚠️  Columns containing only punctuation/symbols: {column_diagnostics['punctuation_only_columns']}")
    if column_diagnostics["special_character_columns"]:
        preview = column_diagnostics["special_character_columns"][:10]
        print(f"  ℹ️  Special-character columns detected and will be referenced with qcol(): {preview}{' ...' if len(column_diagnostics['special_character_columns']) > 10 else ''}")

    if res["total_rows"] == 0:
        print("\n  ⚠️  Source returned 0 rows; skipping row-dependent profiling steps")
        res["score"] = readiness_score(res["null_results"], False, False, res["dup_pct"])
        results[source_key] = res
        df.unpersist()
        continue

    # ── Step 3: Null Rates ────────────────────────────────────────────────────
    print(f"\n  Step 3 — Null Rates")
    null_flags = []
    null_aliases = {col: agg_alias("null", idx) for idx, col in enumerate(df.columns)}
    null_exprs = [
        F.sum(F.when(qcol(col).isNull(), F.lit(1)).otherwise(F.lit(0))).alias(alias)
        for col, alias in null_aliases.items()
    ]
    null_counts = df.agg(*null_exprs).collect()[0].asDict() if null_exprs else {}
    for col in df.columns:
        null_count = null_counts.get(null_aliases[col], 0) or 0
        null_pct   = round(null_count / res["total_rows"] * 100, 2) if res["total_rows"] > 0 else 0
        flag = "⚠️  HIGH" if null_pct > 5 else ("🔶 WARN" if null_pct > 1 else "✅  OK")
        null_flags.append(flag)
        if null_pct > 0:
            print(f"    {col:<45} {null_pct:>6.2f}%  {flag}")
        res["null_results"].append({"col": col, "null_count": null_count, "null_pct": null_pct, "flag": flag})
    ok_count = sum(1 for f in null_flags if "OK" in f)
    print(f"  → {ok_count}/{len(df.columns)} columns fully populated  |  {len(df.columns)-ok_count} with nulls")
>>>>>>> f4a62afb7c099871a80062d0d10ad5eb4194f254

    for col in bus_key_found:
        try:
            freq_df = (
                df.groupBy(col).count()
                  .withColumn("freq_pct", F.round(F.col("count") / res["total_rows"] * 100, 2))
                  .orderBy(F.desc("count"))
                  .limit(50)
            )
            rows = freq_df.collect()
            print(f"\n    {col} — top {min(10, len(rows))} values (of {df.select(col).distinct().count()} distinct):")
            for r in rows[:10]:
                val = str(r[col]) if r[col] is not None else "(null)"
                print(f"      {val:<40} {r['count']:>10,}  {r['freq_pct']:>6.2f}%")

            for r in rows:
                res["domain_profile"].append({
                    "dataset":    source_key,
                    "column":     col,
                    "value":      str(r[col]) if r[col] is not None else "(null)",
                    "frequency":  r["count"],
                    "coverage_pct": r["freq_pct"],
                })
        except Exception as e:
            print(f"    ⚠️  Could not profile {col}: {str(e)[:100]}")

    if not bus_key_found:
        print(f"  ℹ️  No business key columns matched. Check BUSINESS_KEY_COLS list.")

    # ── Step 4: DQ Rules Validation ──────────────────────────────────────────
    print(f"\n  Step 4 — DQ Rules Validation")
    source_rules = DQ_RULES.get(source_key, {})

    if not source_rules:
        print(f"  ℹ️  No DQ rules configured for {source_key} in dq_rules_catalog.yaml")
    else:
        print(f"  {'Column':<40} {'Rule':<20} {'Expected':<20} {'Actual':<20} Status")
        print("  " + "─" * 110)

    for col_name, rules in source_rules.items():
        col_exists = col_name in df.columns
        severity   = rules.get("severity", "MEDIUM")

        for rule_type, rule_val in rules.items():
            if rule_type in ("severity", "description"):
                continue

            expected_str = ""
            actual_str   = ""
            status        = "PASS"

            try:
                if rule_type == "mandatory":
                    if not col_exists:
                        status = "FAIL — COLUMN MISSING"
                        expected_str = "0% null"
                        actual_str   = "column absent"
                    else:
                        col_entry = next((c for c in res["col_inventory"] if c["column"] == col_name), {})
                        npct = col_entry.get("null_pct", 0)
                        expected_str = "0.00% null"
                        actual_str   = f"{npct:.2f}% null"
                        status = "PASS" if npct == 0 else f"FAIL — {npct:.2f}% null"

                elif rule_type == "allowed_values" and col_exists:
                    allowed = rule_val
                    bad_count = df.filter(~F.col(col_name).isin(allowed) & F.col(col_name).isNotNull()).count()
                    expected_str = f"0 violations"
                    actual_str   = f"{bad_count:,} violations"
                    status = "PASS" if bad_count == 0 else f"FAIL — {bad_count:,} bad values"

                elif rule_type == "value_range" and col_exists:
                    mn = rule_val.get("min")
                    mx = rule_val.get("max")
                    oob = 0
                    if mn is not None:
                        oob += df.filter(F.col(col_name) < mn).count()
                    if mx is not None:
                        oob += df.filter(F.col(col_name) > mx).count()
                    expected_str = f"[{mn}, {mx}]"
                    actual_str   = f"{oob:,} out-of-range"
                    status = "PASS" if oob == 0 else f"FAIL — {oob:,} OOB rows"

                elif rule_type == "unique" and rule_val and col_exists:
                    col_entry = next((c for c in res["col_inventory"] if c["column"] == col_name), {})
                    is_unique = col_entry.get("is_pk_candidate", False)
                    expected_str = "all distinct"
                    actual_str   = f"{col_entry.get('distinct_count', '?')} distinct / {res['total_rows']:,} rows"
                    status = "PASS" if is_unique else "FAIL — duplicates exist"

            except Exception as e:
                status = f"ERROR: {str(e)[:60]}"

            pass_flag = "PASS" in status
            if pass_flag:
                res["dq_pass_count"] += 1
            else:
                res["dq_fail_count"] += 1

            severity_icon = {"CRITICAL": "🔴", "HIGH": "⚠️ ", "MEDIUM": "🔶", "LOW": "ℹ️ "}.get(severity, "")
            print(f"  {col_name:<40} {rule_type:<20} {expected_str:<20} {actual_str:<20} "
                  f"{'✅' if pass_flag else severity_icon} {status}")

            res["dq_results"].append({
                "dataset":      source_key,
                "column":       col_name,
                "rule_type":    rule_type,
                "severity":     severity,
                "expected":     expected_str,
                "actual":       actual_str,
                "status":       status,
                "passed":       pass_flag,
            })

    total_dq = res["dq_pass_count"] + res["dq_fail_count"]
    if total_dq > 0:
        print(f"\n  → DQ Summary: {res['dq_pass_count']}/{total_dq} rules PASS  |  "
              f"{res['dq_fail_count']} FAIL")

    # ── Step 5: Temporal Coverage + Gap Detection ─────────────────────────────
    print(f"\n  Step 5 — Temporal Coverage")
    date_col = detect_date_col(df.columns)
    res["date_col"] = date_col

    if date_col:
        stats = df.agg(
            F.min(qcol(date_col)).alias("min"),
            F.max(qcol(date_col)).alias("max"),
            F.countDistinct(qcol(date_col)).alias("distinct")
        ).collect()[0]
        res["date_min"]      = str(stats["min"])
        res["date_max"]      = str(stats["max"])
        res["date_distinct"] = stats["distinct"]
        print(f"  Date column: '{date_col}'")
        print(f"    Range:   {res['date_min']} → {res['date_max']}")
        print(f"    Periods: {res['date_distinct']:,} distinct values")

        # Weekly gap detection
        try:
            field_type = df.schema[date_col].dataType
            if isinstance(field_type, (DateType, TimestampType)):
                w = Window.orderBy("week")
                gap_df = (
                    df.withColumn("week", F.date_trunc("week", F.col(date_col)))
                      .groupBy("week").agg(F.count("*").alias("cnt"))
                      .withColumn("prev_week", F.lag("week").over(w))
                      .withColumn("gap_weeks", F.datediff("week", "prev_week") / 7)
                      .filter(F.col("gap_weeks") > 2)
                )
                res["temporal_gaps"] = gap_df.count()
                if res["temporal_gaps"] > 0:
                    print(f"    ⚠️  {res['temporal_gaps']} gap(s) > 2 consecutive weeks detected")
                    gap_df.select("prev_week", "week", "gap_weeks").show(5, truncate=False)
                else:
                    print(f"    ✅ No temporal gaps > 2 consecutive weeks")
        except Exception as e:
            print(f"    ⚠️  Gap detection skipped: {str(e)[:100]}")
    else:
        print(f"  ⚠️  No date column auto-detected")
        print(f"     Available columns: {', '.join(df.columns[:15])}")

<<<<<<< HEAD
    # ── Step 6: Automatic Grain Detection ────────────────────────────────────
    print(f"\n  Step 6 — Automatic Grain Detection")
    if grain_hint:
        print(f"  Hint from config: '{grain_hint}'")

    # Candidate grain columns: date + business key columns present in dataset
    grain_candidates_pool = [
        c for c in df.columns
        if c.lower() in (BUSINESS_KEY_COLS | {
            "fecha", "day_id", "anio", "mes", "year_id", "fecha_proceso",
            "fecha_venta", "week", "semana", "periodo", "date", "dt"
        })
    ]

    if date_col and date_col not in grain_candidates_pool:
        grain_candidates_pool.insert(0, date_col)

    grain_candidates_pool = grain_candidates_pool[:10]  # cap search space

    print(f"  Candidate pool ({len(grain_candidates_pool)} cols): {grain_candidates_pool}")
    print(f"  {'Candidate Combination':<60} {'Unique %':>10} {'Status':>12}")
    print("  " + "─" * 85)

    best_uniqueness = 0.0
    checked = 0
    MAX_CHECKS = 40  # avoid Cartesian explosion

    for combo_size in range(1, min(6, len(grain_candidates_pool) + 1)):
        for combo in itertools.combinations(grain_candidates_pool, combo_size):
            if checked >= MAX_CHECKS:
                break
            checked += 1
            try:
                deduped = df.dropDuplicates(list(combo)).count()
                uniq_pct = round(deduped / res["total_rows"] * 100, 2) if res["total_rows"] > 0 else 0.0
                confidence = (
                    "✅ PERFECT" if uniq_pct == 100
                    else ("🟡 HIGH" if uniq_pct >= 95
                    else ("🔶 MEDIUM" if uniq_pct >= 80
                    else "🔴 LOW"))
                )
                combo_str = " × ".join(combo)
                print(f"  {combo_str:<60} {uniq_pct:>9.2f}%  {confidence}")

                res["grain_candidates"].append({
                    "dataset":        source_key,
                    "candidate_grain": combo_str,
                    "columns_used":   list(combo),
                    "combo_size":     combo_size,
                    "uniqueness_pct": uniq_pct,
                    "confidence":     confidence,
                    "is_best":        False,
                })

                if uniq_pct > best_uniqueness:
                    best_uniqueness = uniq_pct

                if uniq_pct == 100.0:
                    break  # perfect grain found — stop this combo_size

            except Exception as e:
                print(f"  ⚠️  Skipped {combo}: {str(e)[:80]}")

        if best_uniqueness == 100.0:
            break  # don't try larger combos

    # Mark best candidate
    if res["grain_candidates"]:
        best = max(res["grain_candidates"], key=lambda x: x["uniqueness_pct"])
        best["is_best"] = True
        print(f"\n  ★ Best grain: '{best['candidate_grain']}'  ({best['uniqueness_pct']}%)")

    # ── Step 7: Numeric Volume + Anomaly Flags ────────────────────────────────
    print(f"\n  Step 7 — Numeric Volume")
    num_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, NumericType)]
    print(f"  Numeric columns found: {len(num_cols)}")
    print(f"  {'Column':<40} {'Min':>15} {'Max':>15} {'Sum':>20} {'Negatives':>12}")
    print("  " + "─" * 105)

    for col in num_cols:
        try:
            stats = df.agg(
                F.min(col).alias("mn"), F.max(col).alias("mx"),
                F.sum(col).alias("tot"), F.mean(col).alias("avg")
            ).collect()[0]
            negs  = df.filter(F.col(col) < 0).count()
            zeros = df.filter(F.col(col) == 0).count()
            mn_v  = float(stats["mn"] or 0)
            mx_v  = float(stats["mx"] or 0)
            tot_v = float(stats["tot"] or 0)
            avg_v = float(stats["avg"] or 0)
            neg_flag = f"⚠️  {negs:,}" if negs > 0 else "✅ none"
            print(f"  {col:<40} {mn_v:>15,.1f} {mx_v:>15,.1f} {tot_v:>20,.0f} {neg_flag:>12}")
            res["numeric_stats"].append({
                "dataset": source_key, "col": col,
                "min": mn_v, "max": mx_v, "sum": tot_v, "avg": avg_v,
                "negatives": negs, "zeros": zeros,
            })
        except Exception as e:
            print(f"  ⚠️  {col}: {str(e)[:80]}")

    if not num_cols:
        print(f"  ℹ️  No numeric columns found in this dataset")

    # ── Step 8: Duplicate / PK Analysis ──────────────────────────────────────
    print(f"\n  Step 8 — Duplicate & PK Analysis")
    best_grain = next((g for g in res["grain_candidates"] if g.get("is_best")), None)
    dup_key = best_grain["columns_used"] if best_grain else []

    if len(dup_key) >= 2:
        try:
            deduped   = df.dropDuplicates(dup_key).count()
            dup_count = res["total_rows"] - deduped
            dup_pct   = round(dup_count / res["total_rows"] * 100, 2) if res["total_rows"] > 0 else 0.0
            res["dup_count"] = dup_count
            res["dup_pct"]   = dup_pct
            res["dup_key_used"] = dup_key
            flag = f"⚠️  {dup_count:,} duplicates ({dup_pct}%)" if dup_count > 0 else "✅  No duplicates"
            print(f"  Key: {dup_key}  →  {flag}")
        except Exception as e:
            print(f"  ⚠️  Duplicate check failed: {str(e)[:100]}")
=======
    # ── Step 5: Cardinality ───────────────────────────────────────────────────
    print(f"\n  Step 5 — Key Cardinality")
    id_kw = ["marca", "brand", "cliente", "customer", "canal", "channel",
             "sku", "producto", "product", "categoria", "category",
             "mercado", "market", "region", "zona", "zone", "negocio"]
    key_cols = [c for c in df.columns if any(k in c.lower() for k in id_kw)]
    sampled_key_cols = key_cols[:8]
    if sampled_key_cols:
        card_aliases = {col: agg_alias("card", idx) for idx, col in enumerate(sampled_key_cols)}
        card_stats = df.agg(*[
            F.countDistinct(qcol(col)).alias(alias) for col, alias in card_aliases.items()
        ]).collect()[0].asDict()
        for col in sampled_key_cols:
            n = card_stats.get(card_aliases[col], 0) or 0
            res["cardinality"][col] = n
            print(f"    {col:<45} {n:>8,} distinct")
    else:
        print(f"  ℹ️  No key columns auto-detected — review schema above")

    # ── Step 6: Numeric Stats ─────────────────────────────────────────────────
    print(f"\n  Step 6 — Numeric Volume")
    num_kw = ["ventas", "sales", "units", "unidades", "revenue", "ingreso",
              "inversion", "spend", "monto", "amount", "qty", "cantidad",
              "waste", "merma", "stock", "inv", "precio", "price", "costo", "cost",
              "impr", "impresiones", "click", "ctr", "cpc", "cpm"]
    num_cols = [
        f.name for f in df.schema.fields
        if isinstance(f.dataType, NumericType)
        and any(k in f.name.lower() for k in num_kw)
    ]
    has_numeric = bool(num_cols)
    sampled_num_cols = num_cols[:5]
    if sampled_num_cols:
        num_exprs = []
        num_aliases = {
            col: {stat: agg_alias(f"num_{stat}", idx) for stat in ("mn", "mx", "tot", "neg")}
            for idx, col in enumerate(sampled_num_cols)
        }
        for col in sampled_num_cols:
            aliases = num_aliases[col]
            num_exprs.extend([
                F.min(qcol(col)).alias(aliases["mn"]),
                F.max(qcol(col)).alias(aliases["mx"]),
                F.sum(qcol(col)).alias(aliases["tot"]),
                F.sum(F.when(qcol(col) < 0, F.lit(1)).otherwise(F.lit(0))).alias(aliases["neg"]),
            ])
        numeric_stats = df.agg(*num_exprs).collect()[0].asDict()
        for col in sampled_num_cols:
            aliases = num_aliases[col]
            mn = numeric_stats.get(aliases["mn"])
            mx = numeric_stats.get(aliases["mx"])
            tot = numeric_stats.get(aliases["tot"])
            negs = numeric_stats.get(aliases["neg"], 0) or 0
            neg_flag = f"  ⚠️  {negs:,} negatives" if negs > 0 else ""
            print(f"    {col:<40} min={float(mn or 0):>15,.1f}  max={float(mx or 0):>15,.1f}  total={float(tot or 0):>18,.0f}{neg_flag}")
            res["numeric_stats"].append({
                "col": col, "min": float(mn or 0),
                "max": float(mx or 0), "total": float(tot or 0),
                "negatives": negs
            })
    else:
        print(f"  ℹ️  No numeric metric columns auto-detected")

    # ── Step 7: Duplicate Check ───────────────────────────────────────────────
    print(f"\n  Step 7 — Duplicates")
    nat_key = ([date_col] if date_col else []) + key_cols[:3]
    existing = [c for c in nat_key if c in df.columns]
    if len(existing) >= 2:
        key_projection = [qcol(col).alias(agg_alias("dup_key", idx)) for idx, col in enumerate(existing)]
        deduped   = df.select(*key_projection).distinct().count()
        dup_count = res["total_rows"] - deduped
        dup_pct   = round(dup_count / res["total_rows"] * 100, 2) if res["total_rows"] > 0 else 0
        res["dup_count"] = dup_count
        res["dup_pct"]   = dup_pct
        flag = f"⚠️  {dup_count:,} duplicates ({dup_pct}%)" if dup_count > 0 else "✅  No duplicates"
        print(f"    Key: {existing}  →  {flag}")
>>>>>>> f4a62afb7c099871a80062d0d10ad5eb4194f254
    else:
        print(f"  ℹ️  Best grain has < 2 columns — skipping duplicate check")

    # ── Step 9: Enterprise Readiness Score (6 dimensions) ────────────────────
    print(f"\n  Step 9 — Enterprise Readiness Score")

    # Dimension 1: Completeness (20 pts)
    # Based on mandatory column null violations from DQ rules
    mandatory_fails = sum(
        1 for r in res["dq_results"]
        if r["rule_type"] == "mandatory" and not r["passed"]
    )
    mandatory_total = sum(
        1 for r in res["dq_results"]
        if r["rule_type"] == "mandatory"
    )
    sc_completeness = max(0, 20 - mandatory_fails * 5) if mandatory_total > 0 else 15

    # Dimension 2: Consistency (20 pts)
    # Based on overall DQ rule pass rate (allowed_values + value_range + unique)
    consistency_rules = [r for r in res["dq_results"] if r["rule_type"] != "mandatory"]
    if consistency_rules:
        pass_rate = sum(1 for r in consistency_rules if r["passed"]) / len(consistency_rules)
        sc_consistency = round(pass_rate * 20)
    else:
        sc_consistency = 10  # no rules configured — partial score

    # Dimension 3: Joinability (25 pts) — placeholder, updated by 00c after cross-dataset validation
    # Conservative estimate based on business key null rates
    bk_cols_present = [c for c in res["col_inventory"] if c["column"].lower() in BUSINESS_KEY_COLS]
    if bk_cols_present:
        avg_null = sum(c["null_pct"] for c in bk_cols_present) / len(bk_cols_present)
        sc_joinability = max(0, round(25 - avg_null / 4))
    else:
        sc_joinability = 5

    # Dimension 4: Temporal Coverage (15 pts)
    if res["date_col"]:
        sc_temporal = 15 if res["temporal_gaps"] == 0 else max(5, 15 - res["temporal_gaps"] * 2)
    else:
        sc_temporal = 0

    # Dimension 5: Business Documentation (10 pts)
    total_cols_cnt = len(res["col_inventory"])
    documented_cnt = sum(1 for c in res["col_inventory"] if c["in_glossary"])
    sc_documentation = round((documented_cnt / total_cols_cnt) * 10) if total_cols_cnt > 0 else 0

    # Dimension 6: Grain Clarity (10 pts)
    if best_grain:
        uniq = best_grain["uniqueness_pct"]
        sc_grain = 10 if uniq == 100 else (8 if uniq >= 95 else (6 if uniq >= 80 else 3))
    else:
        sc_grain = 0

    enterprise_score = (sc_completeness + sc_consistency + sc_joinability +
                        sc_temporal + sc_documentation + sc_grain)

    res.update({
        "score_completeness":    sc_completeness,
        "score_consistency":     sc_consistency,
        "score_joinability":     sc_joinability,
        "score_temporal":        sc_temporal,
        "score_documentation":   sc_documentation,
        "score_grain":           sc_grain,
        "enterprise_score":      enterprise_score,
    })

    def score_label(s):
        return "🟢 ENTERPRISE READY" if s >= 80 else ("🟡 CONDITIONAL" if s >= 60 else "🔴 NOT READY")

    print(f"  {'Dimension':<30} {'Score':>8} {'Max':>5}")
    print("  " + "─" * 45)
    print(f"  {'Completeness':<30} {sc_completeness:>8} {20:>5}")
    print(f"  {'Consistency (DQ Rules)':<30} {sc_consistency:>8} {20:>5}")
    print(f"  {'Joinability (estimated)':<30} {sc_joinability:>8} {25:>5}")
    print(f"  {'Temporal Coverage':<30} {sc_temporal:>8} {15:>5}")
    print(f"  {'Business Documentation':<30} {sc_documentation:>8} {10:>5}")
    print(f"  {'Grain Clarity':<30} {sc_grain:>8} {10:>5}")
    print("  " + "─" * 45)
    print(f"  {'ENTERPRISE SCORE':<30} {enterprise_score:>8} {100:>5}  {score_label(enterprise_score)}")

    results[source_key] = res
    df.unpersist()

# COMMAND ----------
<<<<<<< HEAD
# MAGIC %md ## B5 · Summary Scorecard

# COMMAND ----------
print("\n" + "═" * 80)
print("ENTERPRISE READINESS SUMMARY")
print("═" * 80)
print(f"  {'Source':<18} {'Domain':<26} {'Rows':>10}  {'Complete':>9} {'Consist':>8} "
      f"{'Joinabl':>8} {'Temporal':>9} {'DocScore':>9} {'Grain':>7} {'TOTAL':>7}  Status")
print("  " + "─" * 115)

=======

# MAGIC %md ## Summary

# COMMAND ----------

print("\n" + "═" * 70)
print("SUMMARY SCORECARD")
print("═" * 70)
print(f"  {'Source':<18} {'Domain':<28} {'Rows':>12}  {'Score':>6}  Status")
print("  " + "─" * 70)
>>>>>>> f4a62afb7c099871a80062d0d10ad5eb4194f254
for key, res in results.items():
    status = "❌ ERROR" if res["status"] == "ERROR" else (
        "🟢 READY" if res["enterprise_score"] >= 80
        else ("🟡 COND." if res["enterprise_score"] >= 60 else "🔴 NOT READY")
    )
    print(f"  {key:<18} {res['label']:<26} {res['total_rows']:>10,}  "
          f"{res['score_completeness']:>9} {res['score_consistency']:>8} "
          f"{res['score_joinability']:>8} {res['score_temporal']:>9} "
          f"{res['score_documentation']:>9} {res['score_grain']:>7} "
          f"{res['enterprise_score']:>6}/100  {status}")

for key in undefined:
    print(f"  {key:<18} {DOMAIN_LABELS.get(key,key):<26} {'':>10}  {'':>9} {'':>8} {'':>8} {'':>9} {'':>9} {'':>7}  ⏳ TBD")

print(f"\n  Run notebook 00c_enterprise_catalog_writer.py to generate all 10 CSV outputs")
print(f"  and persist results to Snowflake MDP_ANALYTICS.METADATA schema.")

# COMMAND ----------
<<<<<<< HEAD
# MAGIC %md ## B6 · Pass Results to 00c (via notebook exit value or shared storage)

# COMMAND ----------
# The `results` dict is available as a module-level variable.
# When running in sequence via %run, 00c will access it directly.
# If running independently, serialize to a temp Delta table or JSON file.
=======

# MAGIC %md ## Write Output File

# COMMAND ----------

sections = ""
for key, res in results.items():
    if res["status"] == "ERROR":
        sections += f"""
---
## {key} — {res['label']}
**Status:** ❌ ERROR
**SQL:** `{res['sql'].strip()[:200]}`
**Error:** `{res['errors'][0][:300] if res['errors'] else 'Unknown'}`
"""
        continue
>>>>>>> f4a62afb7c099871a80062d0d10ad5eb4194f254

import json

_results_serializable = {}
for k, v in results.items():
    _r = dict(v)
    # Replace un-serializable types
    _r["grain_candidates"] = v.get("grain_candidates", [])
    _results_serializable[k] = _r

<<<<<<< HEAD
try:
    _out_path = f"{OUTPUT_DIR}/_discovery_results_cache.json"
    with open(_out_path, "w", encoding="utf-8") as _f:
        json.dump(_results_serializable, _f, default=str, indent=2)
    print(f"✅ Discovery results cached to: {_out_path}")
    print(f"   → Proceed to run: 00c_enterprise_catalog_writer.py")
except Exception as _e:
    print(f"⚠️  Could not serialize results cache: {_e}")
    print(f"   → Run 00c in the same session immediately after this notebook.")
=======
## {key} — {res['label']}

| | |
|--|--|
| **Readiness** | {res['score']}/100 {score_label(res['score'])} |
| **Database** | `{res['db']}` |
| **Schema** | `{res['schema']}` |
| **Rows** | {res['total_rows']:,} |
| **Columns** | {res['total_cols']} |
| **Date column** | `{res['date_col'] or 'NOT DETECTED'}` |
| **Date range** | {res['date_min'] or 'N/A'} → {res['date_max'] or 'N/A'} ({res['date_distinct'] or 'N/A'} periods) |
| **Duplicates** | {res['dup_count']:,} ({res['dup_pct']}%) |

**SQL:**
```sql
{res['sql'].strip()}
```

### Schema
| Column | Type | Nullable |
|--------|------|---------|
{schema_md}

### Null Rates (non-zero only)
{null_md}

### Key Field Cardinality
| Column | Distinct |
|--------|---------|
{card_md}

### Numeric Volume
| Column | Min | Max | Total | Negatives? |
|--------|-----|-----|-------|-----------|
{num_md}

### Open Items — fill in after reviewing
- [ ] Is the date range correct for this source?
- [ ] Are the key cardinalities plausible?
- [ ] What is the business natural key for deduplication?
- [ ] Does this source need a JOIN with another view?
- [ ] Are there any negative numeric values that need explanation?

"""

pending_md = "\n".join([
    f"| `{k}` | {DOMAIN_LABELS.get(k,k)} | SQL not yet defined |"
    for k in undefined
])
scorecard_md = "\n".join([
    f"| `{k}` | {res['label']} | {res['total_rows']:,} | {res['score']}/100 | {'❌ ERROR' if res['status']=='ERROR' else score_label(res['score'])} |"
    for k, res in results.items()
])

md = f"""# Phase 1 — Source Discovery & Validation

**Generated:** {RUN_AT}  |  **Snowflake:** `{SF_URL}`  |  **Warehouse:** `{SF_WAREHOUSE}`  |  **Role:** `{SF_ROLE}`
**Sources profiled:** {len(results)}/10  |  **Pending:** {len(undefined)}/10

## Scorecard

| Source | Domain | Rows | Score | Status |
|--------|--------|------|-------|--------|
{scorecard_md}

## Pending Sources

| Source | Domain | Status |
|--------|--------|--------|
{pending_md or "_None — all sources defined ✅_"}

{sections}

---

## Next Step

1. Fill in **Open Items** above for each source
2. Add SQL for pending sources and re-run
3. `git add docs/phase_outputs/phase1_data_inventory.md`
4. `git commit -m "data: source discovery {len(results)}/10 sources profiled"`
5. `git push origin main`
6. Tell the agent: **"discovery done, inventory committed"**
"""

os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    f.write(md)

print(f"\n{'═'*70}")
print(f"✅ Output written: {OUTPUT_FILE}")
print(f"{'═'*70}")
print("\ngit add docs/phase_outputs/phase1_data_inventory.md")
print('git commit -m "data: source discovery"')
print("git push origin main")
>>>>>>> f4a62afb7c099871a80062d0d10ad5eb4194f254
