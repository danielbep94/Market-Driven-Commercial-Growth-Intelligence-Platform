# Databricks notebook source
# MAGIC %md
# MAGIC # 00b · Source Discovery & Validation
# MAGIC
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
# MAGIC
# MAGIC Each source is a dict with:
# MAGIC - `"db"`:     Snowflake database (e.g. `"PRD_MDP"`)
# MAGIC - `"schema"`: Default schema for this source (e.g. `"MDP_DSP"`)
# MAGIC - `"sql"`:    Query — use `TABLE`, `SCHEMA.TABLE`, or `DB.SCHEMA.TABLE` as needed for the active Snowflake context
# MAGIC
# MAGIC Set value to `None` for sources not yet ready.

# COMMAND ----------

SOURCES = {

    # ── 1 · Investment / Marketing ──────────────────────────────────────────
    "DATA_MKT": {
        "db":     "PRD_MDP",
        "schema": "MDP_DSP",
        "sql":    "SELECT * FROM PRD_MDP.MDP_DSP.VW_MKT_ECOMM WHERE anio >= 2024",
    },

    # ── 2 · Sell-In ──────────────────────────────────────────────────────────
    "DATA_SELL_IN": None,   # ← replace None with dict when ready

    # ── 3 · Sell-Out ─────────────────────────────────────────────────────────
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

    "DATA_SELL_OUT": None,

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

# MAGIC %md ## ─── DO NOT EDIT BELOW ───────────────────────────────────────────

# COMMAND ----------

# MAGIC %md ## Connection

# COMMAND ----------

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

from datetime import datetime
import os
import re
from collections import Counter
import pyspark.sql.functions as F
from pyspark.sql.types import NumericType

RUN_AT      = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
OUTPUT_FILE = "docs/phase_outputs/phase1_data_inventory.md"

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
    }

def validate_source_config(source_key: str, cfg: dict):
    """Fail fast on incomplete source definitions instead of surfacing opaque connector errors."""
    if not isinstance(cfg, dict):
        raise ValueError(f"{source_key}: source config must be a dict or None")
    missing = [key for key in ("db", "schema", "sql") if not cfg.get(key)]
    if missing:
        raise ValueError(f"{source_key}: missing required config key(s): {', '.join(missing)}")

def run_sql(db: str, schema: str, sql: str):
    """
    Execute SQL against Snowflake using the requested database/schema/role context.
    SQL may use TABLE, SCHEMA.TABLE, or DB.SCHEMA.TABLE depending on
    how Snowflake resolves the object for the active user and role.
    """
    if not sql or not str(sql).strip():
        raise ValueError("SQL query is empty")
    return spark.read.format("snowflake") \
               .options(**get_sf_opts(db, schema)) \
               .option("query", str(sql).strip()) \
               .load()

def detect_date_col(columns: list):
    priority = ["anio", "año", "year", "yr", "periodo", "period",
                "fecha", "fecha_proceso", "fecha_venta", "fecha_cierre",
                "date", "dt", "week", "semana", "mes", "month"]
    col_lower = {c.strip().lower(): c for c in columns if isinstance(c, str)}
    for p in priority:
        if p in col_lower:
            return col_lower[p]
    return None

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

# COMMAND ----------

defined   = {k: v for k, v in SOURCES.items() if v is not None}
undefined = {k: v for k, v in SOURCES.items() if v is None}

print(f"Sources defined:   {len(defined)}/10")
print(f"Sources pending:   {len(undefined)}/10")
if undefined:
    print(f"  Pending: {', '.join(undefined.keys())}")

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

for source_key, cfg in defined.items():
    label = DOMAIN_LABELS.get(source_key, source_key)
    db, schema, sql = cfg["db"], cfg["schema"], cfg["sql"]

    print("\n" + "═" * 70)
    print(f"  {source_key}  —  {label}")
    print("═" * 70)
    print(f"  db={db}  schema={schema}")
    print(f"  SQL: {sql.strip()[:100]}{'...' if len(sql.strip()) > 100 else ''}\n")

    res = {
        "key": source_key, "label": label, "db": db, "schema": schema, "sql": sql,
        "status": "OK", "total_rows": 0, "total_cols": 0,
        "schema_rows": [], "null_results": [], "date_col": None,
        "date_min": None, "date_max": None, "date_distinct": None,
        "cardinality": {}, "numeric_stats": [],
        "dup_count": 0, "dup_pct": 0.0, "score": 0,
        "errors": [], "sample_rows": [],
    }

    # ── Step 1: Execute ───────────────────────────────────────────────────────
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
        results[source_key] = res
        continue

    # ── Step 2: Schema ────────────────────────────────────────────────────────
    print(f"\n  Step 2 — Schema ({res['total_cols']} columns)")
    print(f"  {'Column':<45} {'Type':<25} Nullable")
    print("  " + "─" * 75)
    for field in df.schema.fields:
        print(f"  {field.name:<45} {str(field.dataType):<25} {'NULL' if field.nullable else 'NOT NULL'}")
        res["schema_rows"].append({
            "name": field.name, "type": str(field.dataType), "nullable": field.nullable
        })

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

    # ── Step 4: Temporal Coverage ─────────────────────────────────────────────
    print(f"\n  Step 4 — Temporal Coverage")
    date_col = detect_date_col(df.columns)
    res["date_col"] = date_col
    if date_col:
        stats = df.agg(
            F.min(qcol(date_col)).alias("min"),
            F.max(qcol(date_col)).alias("max"),
            F.countDistinct(qcol(date_col)).alias("distinct")
        ).collect()[0]
        res["date_min"] = str(stats["min"])
        res["date_max"] = str(stats["max"])
        res["date_distinct"] = stats["distinct"]
        print(f"  Date column: '{date_col}'")
        print(f"    Range:   {res['date_min']} → {res['date_max']}")
        print(f"    Periods: {res['date_distinct']:,} distinct values")
    else:
        print(f"  ⚠️  No date column auto-detected")
        print(f"     Columns available: {', '.join(df.columns[:15])}")

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
    else:
        print(f"  ℹ️  Not enough key columns to auto-check (need ≥ 2)")

    # ── Step 8: Sample ────────────────────────────────────────────────────────
    print(f"\n  Step 8 — Sample rows")
    sample = df.limit(5).collect()
    res["sample_rows"] = [r.asDict() for r in sample]
    if sample:
        first = sample[0].asDict()
        for k, v in list(first.items())[:10]:
            print(f"    {k:<40} = {v}")
        if len(first) > 10:
            print(f"    ... +{len(first)-10} more columns")

    # ── Score ─────────────────────────────────────────────────────────────────
    res["score"] = readiness_score(res["null_results"], date_col is not None, has_numeric, res["dup_pct"])
    print(f"\n  {'─' * 68}")
    print(f"  READINESS: {res['score']}/100  {score_label(res['score'])}")
    print(f"  {'─' * 68}")
    results[source_key] = res
    df.unpersist()

# COMMAND ----------

# MAGIC %md ## Summary

# COMMAND ----------

print("\n" + "═" * 70)
print("SUMMARY SCORECARD")
print("═" * 70)
print(f"  {'Source':<18} {'Domain':<28} {'Rows':>12}  {'Score':>6}  Status")
print("  " + "─" * 70)
for key, res in results.items():
    status = "❌ ERROR" if res["status"] == "ERROR" else score_label(res["score"])
    print(f"  {key:<18} {res['label']:<28} {res['total_rows']:>12,}  {res['score']:>5}/100  {status}")
for key in undefined:
    print(f"  {key:<18} {DOMAIN_LABELS.get(key,key):<28} {'':>12}  {'':>6}  ⏳ TBD")

# COMMAND ----------

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

    schema_md = "\n".join([
        f"| `{r['name']}` | `{r['type']}` | {'✓' if r['nullable'] else '✗'} |"
        for r in res["schema_rows"]
    ])
    null_md = "\n".join([
        f"| `{n['col']}` | {n['null_count']:,} | {n['null_pct']}% | {n['flag']} |"
        for n in res["null_results"] if n["null_pct"] > 0
    ]) or "_All columns fully populated ✅_"
    card_md = "\n".join([
        f"| `{col}` | {n:,} |" for col, n in res["cardinality"].items()
    ]) or "_No key columns auto-detected — fill in manually_"
    num_md = "\n".join([
        f"| `{s['col']}` | {s['min']:,.1f} | {s['max']:,.1f} | {s['total']:,.0f} | {'⚠️' if s['negatives']>0 else '✅'} |"
        for s in res["numeric_stats"]
    ]) or "_No numeric columns auto-detected_"

    sections += f"""
---

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
