# Databricks notebook source
# MAGIC %md
# MAGIC # 00b · Snowflake Discovery — Map What Exists
# MAGIC
# MAGIC **Purpose:** Before building any Bronze tables, understand exactly what raw data
# MAGIC already exists in Snowflake — databases, schemas, tables, row counts, and column names.
# MAGIC
# MAGIC **Run this before any other profiling notebook.**
# MAGIC Output is written to `docs/phase_outputs/phase1_data_inventory.md`
# MAGIC Commit and push — the agent reads it to plan the Bronze table SQL.

# COMMAND ----------
# MAGIC %md ## Configuration

# COMMAND ----------
# ── Azure Key Vault ──────────────────────────────────────────────────────────
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
    print("🚨 Non-Databricks env — using MOCK credentials")
    user, password = "MOCK_USER", "MOCK_PASSWORD"
except Exception as e:
    print(f"🚨 Secret error: {e}")
    user, password = "MOCK_USER", "MOCK_PASSWORD"

OUTPUT_FILE = "docs/phase_outputs/phase1_data_inventory.md"

from datetime import datetime
RUN_AT = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

def sf_query(db, schema, sql):
    """Run any SQL against Snowflake and return a Spark DataFrame."""
    opts = {
        "sfURL": SF_URL, "sfUser": user, "sfPassword": password,
        "sfDatabase": db, "sfSchema": schema, "sfWarehouse": SF_WAREHOUSE,
    }
    return spark.read.format("snowflake").options(**opts).option("query", sql).load()

# COMMAND ----------
# MAGIC %md ## 1. List All Databases You Have Access To

# COMMAND ----------
print("=" * 60)
print("STEP 1 — Databases accessible to this user")
print("=" * 60)

try:
    dbs_df = sf_query("SNOWFLAKE", "INFORMATION_SCHEMA",
                      "SELECT DATABASE_NAME, CREATED FROM SNOWFLAKE.INFORMATION_SCHEMA.DATABASES ORDER BY DATABASE_NAME")
    db_rows = dbs_df.collect()
    databases = [r["DATABASE_NAME"] for r in db_rows]
    for db in databases:
        print(f"  📦 {db}")
    print(f"\nTotal databases: {len(databases)}")
except Exception as e:
    # Fallback: SHOW DATABASES
    print(f"  (INFORMATION_SCHEMA approach failed: {str(e)[:100]})")
    print("  Trying SHOW DATABASES...")
    try:
        dbs_df = sf_query("SNOWFLAKE", "INFORMATION_SCHEMA", "SHOW DATABASES")
        dbs_df.select("name").show(50, truncate=False)
        databases = [r["name"] for r in dbs_df.select("name").collect()]
    except Exception as e2:
        print(f"  ❌ Could not list databases: {e2}")
        databases = []

# COMMAND ----------
# MAGIC %md ## 2. Identify the Relevant Database
# MAGIC
# MAGIC **⚠️ ACTION REQUIRED:** Look at the database list above.
# MAGIC Set `TARGET_DB` to the database that contains your commercial data
# MAGIC (sell-in, sell-out, waste, investment, Nielsen, etc.)

# COMMAND ----------
# ── UPDATE THIS after reviewing the database list above ──────────────────────
TARGET_DB = "PLEASE_SET_THIS"   # ← e.g. "MDP_PRD" or "DANONE_COMMERCIAL_DW"
# ─────────────────────────────────────────────────────────────────────────────

if TARGET_DB == "PLEASE_SET_THIS":
    print("⚠️  Update TARGET_DB above, then re-run from this cell.")
    dbutils.notebook.exit("TARGET_DB_NOT_SET")

print(f"Target database: {TARGET_DB}")

# COMMAND ----------
# MAGIC %md ## 3. List All Schemas in Target Database

# COMMAND ----------
print(f"\n{'=' * 60}")
print(f"STEP 3 — Schemas in {TARGET_DB}")
print("=" * 60)

try:
    schemas_df = sf_query(TARGET_DB, "INFORMATION_SCHEMA",
                          f"SELECT SCHEMA_NAME FROM {TARGET_DB}.INFORMATION_SCHEMA.SCHEMATA ORDER BY SCHEMA_NAME")
    schema_rows = schemas_df.collect()
    schemas = [r["SCHEMA_NAME"] for r in schema_rows]
    for s in schemas:
        print(f"  📂 {s}")
    print(f"\nTotal schemas: {len(schemas)}")
except Exception as e:
    print(f"  Fallback to SHOW SCHEMAS: {str(e)[:100]}")
    schemas_df = sf_query(TARGET_DB, "INFORMATION_SCHEMA", f"SHOW SCHEMAS IN DATABASE {TARGET_DB}")
    schemas_df.select("name").show(50, truncate=False)
    schemas = [r["name"] for r in schemas_df.select("name").collect()]

# COMMAND ----------
# MAGIC %md ## 4. List All Tables Across All Schemas (with Row Counts)
# MAGIC
# MAGIC This is the master table inventory. Look for tables related to:
# MAGIC sell-in, sell-out, waste, investment/spend, forecast, Nielsen/market share,
# MAGIC price, promotions, inventory, and calendar/date.

# COMMAND ----------
print(f"\n{'=' * 60}")
print(f"STEP 4 — All tables in {TARGET_DB}")
print("=" * 60)

try:
    tables_df = sf_query(TARGET_DB, "INFORMATION_SCHEMA", f"""
        SELECT
            TABLE_SCHEMA,
            TABLE_NAME,
            TABLE_TYPE,
            ROW_COUNT,
            BYTES,
            LAST_ALTERED
        FROM {TARGET_DB}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA')
        ORDER BY TABLE_SCHEMA, TABLE_NAME
    """)

    table_rows = tables_df.collect()
    print(f"Total tables/views: {len(table_rows)}\n")

    current_schema = ""
    table_inventory = []
    for r in table_rows:
        if r["TABLE_SCHEMA"] != current_schema:
            current_schema = r["TABLE_SCHEMA"]
            print(f"\n  📂 {current_schema}")
        row_count = f"{int(r['ROW_COUNT']):,}" if r["ROW_COUNT"] else "N/A"
        size_mb   = f"{int(r['BYTES']) / 1024 / 1024:.1f} MB" if r["BYTES"] else "N/A"
        print(f"     {'[VIEW]  ' if r['TABLE_TYPE'] == 'VIEW' else '[TABLE] '}"
              f"{r['TABLE_NAME']:<60} rows={row_count:<15} size={size_mb}")
        table_inventory.append({
            "schema": r["TABLE_SCHEMA"],
            "name": r["TABLE_NAME"],
            "type": r["TABLE_TYPE"],
            "rows": r["ROW_COUNT"],
            "bytes": r["BYTES"]
        })

except Exception as e:
    print(f"INFORMATION_SCHEMA.TABLES failed: {str(e)[:200]}")
    print("Trying per-schema SHOW TABLES...")
    table_inventory = []
    for schema in schemas:
        try:
            t = sf_query(TARGET_DB, schema, f"SHOW TABLES IN {TARGET_DB}.{schema}")
            for r in t.collect():
                print(f"  [{schema}] {r['name']}")
                table_inventory.append({"schema": schema, "name": r["name"], "type": "TABLE", "rows": None, "bytes": None})
        except:
            pass

# COMMAND ----------
# MAGIC %md ## 5. Keyword Search — Find Relevant Tables
# MAGIC
# MAGIC Searches table names for keywords related to our 10 data domains.

# COMMAND ----------
KEYWORDS = {
    "sell_in":    ["sell_in", "sellin", "sell-in", "invoice", "shipment", "dispatch", "despacho"],
    "sell_out":   ["sell_out", "sellout", "sell-out", "pdv", "pos", "point_of_sale", "offtake"],
    "waste":      ["waste", "merma", "expired", "expiry", "caducidad", "devoluc"],
    "investment": ["invest", "spend", "trade", "marketing", "tts", "promotional_spend"],
    "forecast":   ["forecast", "pronostico", "demand_plan", "projection"],
    "nielsen":    ["nielsen", "market_share", "share", "syndicated"],
    "price":      ["price", "precio", "tariff", "pricepoint"],
    "promotions": ["promo", "promotion", "descuento", "discount", "mechanic"],
    "inventory":  ["inventory", "stock", "inventario", "warehouse", "almacen"],
    "calendar":   ["calendar", "date", "dim_date", "periodo", "fiscal"],
}

print(f"\n{'=' * 60}")
print("STEP 5 — Keyword Match (which tables likely contain our data)")
print("=" * 60)

matches = {domain: [] for domain in KEYWORDS}
for t in table_inventory:
    full_name = f"{t['schema']}.{t['name']}".lower()
    for domain, kws in KEYWORDS.items():
        if any(kw in full_name for kw in kws):
            matches[domain].append(f"{t['schema']}.{t['name']}")
            break

for domain, found in matches.items():
    status = "✅" if found else "❓ NOT FOUND"
    print(f"\n  {domain.upper():<15} {status}")
    for f in found:
        print(f"               → {f}")

# COMMAND ----------
# MAGIC %md ## 6. Sample 3 Rows from Each Matched Table
# MAGIC
# MAGIC Quick look at actual data to confirm column names and formats.

# COMMAND ----------
print(f"\n{'=' * 60}")
print("STEP 6 — Sample rows from matched tables")
print("=" * 60)

sample_results = {}
for domain, table_list in matches.items():
    if not table_list:
        continue
    t_full = table_list[0]  # Use first match
    schema, table = t_full.split(".", 1)
    print(f"\n  [{domain.upper()}] {t_full}")
    print("  " + "-" * 56)
    try:
        sample_df = sf_query(TARGET_DB, schema, f"SELECT * FROM {table} LIMIT 3")
        sample_cols = sample_df.columns
        sample_rows = sample_df.collect()
        print(f"  Columns: {', '.join(sample_cols[:10])}{'...' if len(sample_cols) > 10 else ''}")
        for row in sample_rows:
            print(f"  {dict(list(row.asDict().items())[:6])}")
        sample_results[domain] = {
            "table": t_full,
            "columns": sample_cols,
            "sample": [r.asDict() for r in sample_rows]
        }
    except Exception as e:
        print(f"  ⚠️  Could not sample: {str(e)[:100]}")

# COMMAND ----------
# MAGIC %md ## 7. Write Output to Markdown

# COMMAND ----------
# Build table inventory markdown
table_md_rows = "\n".join([
    f"| `{t['schema']}` | `{t['name']}` | {t['type']} | {int(t['rows']):,} |" if t['rows']
    else f"| `{t['schema']}` | `{t['name']}` | {t['type']} | N/A |"
    for t in table_inventory
])

# Build domain match markdown
domain_rows = "\n".join([
    f"| **{domain.upper()}** | {chr(10).join(tables) if tables else '❓ Not found'} |"
    for domain, tables in matches.items()
])

# Build column inventory for matched tables
col_sections = ""
for domain, info in sample_results.items():
    col_list = "\n".join([f"| `{c}` | [TBD] | [TBD] | [TBD] |" for c in info["columns"]])
    col_sections += f"""
### {domain.upper()} — `{info['table']}`

| Column | Type | Nullable | Notes |
|--------|------|----------|-------|
{col_list}

"""

output_md = f"""# Phase 1 — Snowflake Data Inventory

**Generated:** {RUN_AT}
**Target Database:** `{TARGET_DB}`
**Snowflake URL:** `{SF_URL}`

---

## All Tables in {TARGET_DB}

| Schema | Table | Type | Row Count |
|--------|-------|------|-----------|
{table_md_rows}

---

## Domain Match — Which Tables Map to Our 10 Sources

| Domain | Candidate Table(s) |
|--------|--------------------|
{domain_rows}

---

## Column Inventory — Candidate Tables

{col_sections}

---

## Open Items — Fill in after reviewing

For each domain, confirm or correct the candidate table:

| Domain | Candidate Table | Confirmed? | Notes |
|--------|----------------|-----------|-------|
| SELL_IN | | | |
| SELL_OUT | | | |
| WASTE | | | |
| INVESTMENT | | | |
| FORECAST | | | |
| NIELSEN | | | |
| PRICE | | | |
| PROMOTIONS | | | |
| INVENTORY | | | |
| CALENDAR | | | |

---

## Next Step

Once the table above is filled in, commit and push this file.
Tell the agent: "discovery done, inventory committed"
The agent will generate the Bronze DDL SQL for each confirmed table.
"""

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    f.write(output_md)

print(f"\n{'=' * 60}")
print(f"✅ Discovery complete. Output written to:")
print(f"   {OUTPUT_FILE}")
print(f"\nNow:")
print(f"  1. Review the output above")
print(f"  2. Fill in the 'Open Items' table in {OUTPUT_FILE}")
print(f"  3. git add docs/phase_outputs/phase1_data_inventory.md")
print(f"  4. git commit -m 'data: snowflake discovery'")
print(f"  5. git push origin main")
print(f"  6. Tell the agent: 'discovery done, inventory committed'")
print(f"{'=' * 60}")
