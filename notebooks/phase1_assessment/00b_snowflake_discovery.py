# Databricks notebook source
# MAGIC %md
# MAGIC # 00b · Snowflake Discovery — PRD_MDP.MDP_DSP
# MAGIC
# MAGIC **What this does:**
# MAGIC 1. Lists every view/table available in PRD_MDP.MDP_DSP
# MAGIC 2. Shows column names + types for each view
# MAGIC 3. Samples 5 rows (filtered anio >= 2024 where applicable)
# MAGIC 4. Writes a full catalog to docs/phase_outputs/phase1_data_inventory.md
# MAGIC
# MAGIC **After running:** commit the output file and push.
# MAGIC Tell the agent: "discovery done, inventory committed"
# MAGIC The agent reads the catalog and writes the Bronze extraction SQL for each source.

# COMMAND ----------
# MAGIC %md ## Connection

# COMMAND ----------
KEYVAULT_NAME = "DAN-AM-P-KVT800-R-MDP-DB"
KEY_NAME_USR  = "snowflake-user"
KEY_NAME_PWD  = "snowflake-password"
SF_URL        = "danonenam.east-us-2.azure.snowflakecomputing.com"
SF_WAREHOUSE  = "PRD_MDP_ANL_WH"

# ── Source coordinates ────────────────────────────────────────────────────────
TARGET_DB     = "PRD_MDP"
TARGET_SCHEMA = "MDP_DSP"
YEAR_FILTER   = 2024       # anio >= YEAR_FILTER applied wherever anio column exists

try:
    user     = dbutils.secrets.get(scope=KEYVAULT_NAME, key=KEY_NAME_USR)
    password = dbutils.secrets.get(scope=KEYVAULT_NAME, key=KEY_NAME_PWD)
    print(f"✅ Credentials loaded from: {KEYVAULT_NAME}")
except NameError:
    user, password = "MOCK_USER", "MOCK_PASSWORD"
    print("🚨 MOCK credentials — not in Databricks")
except Exception as e:
    user, password = "MOCK_USER", "MOCK_PASSWORD"
    print(f"🚨 Secret error: {e}")

from datetime import datetime
RUN_AT = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
OUTPUT_FILE = "docs/phase_outputs/phase1_data_inventory.md"

BASE_OPTS = {
    "sfURL": SF_URL, "sfUser": user, "sfPassword": password,
    "sfDatabase": TARGET_DB, "sfSchema": TARGET_SCHEMA,
    "sfWarehouse": SF_WAREHOUSE,
}

def run_sql(sql, schema=TARGET_SCHEMA):
    opts = {**BASE_OPTS, "sfSchema": schema}
    return spark.read.format("snowflake").options(**opts).option("query", sql).load()

# COMMAND ----------
# MAGIC %md ## 1. List All Views & Tables in MDP_DSP

# COMMAND ----------
print("=" * 65)
print(f"ALL OBJECTS IN {TARGET_DB}.{TARGET_SCHEMA}")
print("=" * 65)

catalog_df = run_sql(f"""
    SELECT
        TABLE_NAME,
        TABLE_TYPE,
        ROW_COUNT,
        BYTES,
        LAST_ALTERED,
        COMMENT
    FROM {TARGET_DB}.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = '{TARGET_SCHEMA}'
      AND TABLE_NAME NOT LIKE 'STAGE_%'
    ORDER BY TABLE_TYPE DESC, TABLE_NAME
""")

all_objects = catalog_df.collect()
print(f"Total objects: {len(all_objects)}\n")

views  = [r for r in all_objects if r["TABLE_TYPE"] == "VIEW"]
tables = [r for r in all_objects if r["TABLE_TYPE"] == "BASE TABLE"]

print(f"  Views:  {len(views)}")
print(f"  Tables: {len(tables)}")
print("")

print(f"{'Name':<55} {'Type':<10} {'Rows':>12}  Last Altered")
print("-" * 100)
for r in all_objects:
    rows = f"{int(r['ROW_COUNT']):,}" if r["ROW_COUNT"] else "N/A"
    obj_type = "VIEW" if r["TABLE_TYPE"] == "VIEW" else "TABLE"
    print(f"  {r['TABLE_NAME']:<53} {obj_type:<10} {rows:>12}  {str(r['LAST_ALTERED'])[:10]}")

# COMMAND ----------
# MAGIC %md ## 2. Also List Other Schemas in PRD_MDP
# MAGIC
# MAGIC Some sources may live in other schemas — we catalog everything.

# COMMAND ----------
print("\n" + "=" * 65)
print(f"ALL SCHEMAS IN {TARGET_DB}")
print("=" * 65)

other_schemas_df = run_sql(f"""
    SELECT SCHEMA_NAME, CREATED
    FROM {TARGET_DB}.INFORMATION_SCHEMA.SCHEMATA
    WHERE SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA', 'PUBLIC')
    ORDER BY SCHEMA_NAME
""", schema="INFORMATION_SCHEMA")

all_schemas = [r["SCHEMA_NAME"] for r in other_schemas_df.collect()]
for s in all_schemas:
    marker = " ← (currently exploring)" if s == TARGET_SCHEMA else ""
    print(f"  📂 {s}{marker}")

# COMMAND ----------
# MAGIC %md ## 3. Column Inventory for Every View in MDP_DSP

# COMMAND ----------
print("\n" + "=" * 65)
print("COLUMN INVENTORY — ALL VIEWS")
print("=" * 65)
print("(This is what the agent uses to build the Bronze extraction SQL)")

column_catalog = {}

for r in all_objects:
    obj_name = r["TABLE_NAME"]
    print(f"\n{'─' * 65}")
    print(f"  {r['TABLE_TYPE']}: {TARGET_DB}.{TARGET_SCHEMA}.{obj_name}")
    print(f"{'─' * 65}")
    try:
        cols_df = run_sql(f"""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COMMENT
            FROM {TARGET_DB}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{TARGET_SCHEMA}'
              AND TABLE_NAME   = '{obj_name}'
            ORDER BY ORDINAL_POSITION
        """)
        cols = cols_df.collect()
        column_catalog[obj_name] = cols
        for c in cols:
            nullable = "NULL" if c["IS_NULLABLE"] == "YES" else "NOT NULL"
            comment  = f"  -- {c['COMMENT']}" if c["COMMENT"] else ""
            print(f"  {c['COLUMN_NAME']:<45} {c['DATA_TYPE']:<20} {nullable}{comment}")
        print(f"  → {len(cols)} columns total")
    except Exception as e:
        print(f"  ⚠️  Could not get columns: {str(e)[:120]}")
        column_catalog[obj_name] = []

# COMMAND ----------
# MAGIC %md ## 4. Sample 5 Rows — With anio >= 2024 Filter

# COMMAND ----------
print("\n" + "=" * 65)
print("SAMPLE ROWS (anio >= 2024 where column exists)")
print("=" * 65)

sample_catalog = {}

for r in all_objects:
    obj_name = r["TABLE_NAME"]
    cols     = column_catalog.get(obj_name, [])
    col_names = [c["COLUMN_NAME"].upper() for c in cols]

    has_year  = "ANIO"        in col_names
    has_fecha = "FECHA"       in col_names or "FECHA_PROCESO" in col_names
    date_col  = next((c for c in col_names if "ANIO" in c), None)

    year_clause = f"WHERE {date_col} >= {YEAR_FILTER}" if has_year and date_col else ""

    print(f"\n  {obj_name}  {f'(filter: {year_clause})' if year_clause else '(no anio column — no filter)'}")
    try:
        sample_df = run_sql(f"SELECT * FROM {TARGET_SCHEMA}.{obj_name} {year_clause} LIMIT 5")
        sample_rows = sample_df.collect()
        sample_catalog[obj_name] = {
            "columns": sample_df.columns,
            "rows":    [r.asDict() for r in sample_rows],
            "year_filter": year_clause
        }
        # Print first row as column:value pairs for readability
        if sample_rows:
            first = sample_rows[0].asDict()
            for col_name, val in list(first.items())[:12]:
                print(f"    {col_name:<40} = {val}")
            if len(first) > 12:
                print(f"    ... +{len(first) - 12} more columns")
    except Exception as e:
        print(f"  ⚠️  Could not sample: {str(e)[:150]}")
        sample_catalog[obj_name] = {"columns": [], "rows": [], "year_filter": year_clause}

# COMMAND ----------
# MAGIC %md ## 5. Domain Keyword Match
# MAGIC
# MAGIC Suggests which view maps to which of the 10 platform data sources.
# MAGIC **You confirm or correct in the output file.**

# COMMAND ----------
DOMAIN_KEYWORDS = {
    "SELL_IN":     ["sell_in", "sellin", "invoice", "shipment", "dispatch", "despacho", "factura", "venta_neta", "ventas"],
    "SELL_OUT":    ["sell_out", "sellout", "pdv", "pos", "offtake", "retail", "ecomm", "puntos_de_venta"],
    "WASTE":       ["waste", "merma", "expired", "caducidad", "devolucion", "devoluc"],
    "INVESTMENT":  ["mkt", "marketing", "invest", "spend", "trade", "tts", "inversion", "media"],
    "FORECAST":    ["forecast", "pronostico", "demand", "proyeccion", "plan"],
    "NIELSEN":     ["nielsen", "market_share", "share", "syndicated", "kantar"],
    "PRICE":       ["price", "precio", "tariff", "pricepoint", "precios"],
    "PROMOTIONS":  ["promo", "promotion", "descuento", "discount", "mecanica"],
    "INVENTORY":   ["inventory", "stock", "inventario", "warehouse", "almacen", "whs"],
    "CALENDAR":    ["calendar", "date", "dim_date", "periodo", "fiscal", "dim_tiempo"],
}

print("\n" + "=" * 65)
print("DOMAIN MATCH — Suggested mapping (confirm or correct in output file)")
print("=" * 65)

domain_matches = {d: [] for d in DOMAIN_KEYWORDS}
for r in all_objects:
    name_lower = r["TABLE_NAME"].lower()
    for domain, keywords in DOMAIN_KEYWORDS.items():
        if any(kw in name_lower for kw in keywords):
            domain_matches[domain].append(r["TABLE_NAME"])

for domain, matches in domain_matches.items():
    status = "✅" if matches else "❓"
    print(f"  {status} {domain:<15} → {', '.join(matches) if matches else 'NOT FOUND — may need cross-table join'}")

# COMMAND ----------
# MAGIC %md ## 6. Write Output Catalog

# COMMAND ----------
# Build column tables per view
col_sections = ""
for obj_name, cols in column_catalog.items():
    if not cols:
        continue
    col_rows = "\n".join([
        f"| `{c['COLUMN_NAME']}` | `{c['DATA_TYPE']}` | {'✓' if c['IS_NULLABLE'] == 'YES' else '✗'} | {c['COMMENT'] or ''} |"
        for c in cols
    ])
    sample_info = sample_catalog.get(obj_name, {})
    year_note = f" — filtered `{sample_info.get('year_filter', 'none')}`" if sample_info.get('year_filter') else ""
    col_sections += f"""
### `{TARGET_SCHEMA}.{obj_name}`{year_note}

| Column | Type | Nullable | Comment |
|--------|------|---------|---------|
{col_rows}

"""

# Domain mapping table for manual confirmation
domain_map_rows = "\n".join([
    f"| **{domain}** | {', '.join([f'`{m}`' for m in matches]) if matches else '❓ Not found'} | | |"
    for domain, matches in domain_matches.items()
])

# All objects table
obj_rows = "\n".join([
    f"| `{r['TABLE_NAME']}` | {'VIEW' if r['TABLE_TYPE'] == 'VIEW' else 'TABLE'} | {f\"{int(r['ROW_COUNT']):,}\" if r['ROW_COUNT'] else 'N/A'} | {str(r['LAST_ALTERED'])[:10]} |"
    for r in all_objects
])

output_md = f"""# Phase 1 — Snowflake Data Inventory

**Generated:** {RUN_AT}
**Source database:** `{TARGET_DB}`
**Source schema:** `{TARGET_SCHEMA}`
**Year filter:** `anio >= {YEAR_FILTER}`
**Snowflake URL:** `{SF_URL}`

---

## All Objects in {TARGET_DB}.{TARGET_SCHEMA}

| Name | Type | Row Count | Last Altered |
|------|------|-----------|-------------|
{obj_rows}

---

## Domain Mapping — Confirm or Correct

For each domain, confirm which view/table to use as the source.
Some domains may require JOINs across multiple views — document the join logic in the Notes column.

| Domain | Candidate View(s) | Confirmed? | Notes / Join Logic |
|--------|--------------------|-----------|-------------------|
{domain_map_rows}

---

## Other Schemas in {TARGET_DB}

{chr(10).join([f'- `{s}`' for s in all_schemas])}

---

## Full Column Inventory
{col_sections}

---

## Next Step

1. Fill in the **Domain Mapping** table above (Confirmed? + Notes columns)
2. For any domain with no candidate view, document the JOIN logic needed
3. Commit and push this file
4. Tell the agent: "discovery done, inventory committed"

The agent will generate Bronze extraction SQL for every confirmed domain.
"""

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    f.write(output_md)

print("\n" + "=" * 65)
print("✅ Discovery complete")
print(f"   Output: {OUTPUT_FILE}")
print("=" * 65)
print("")
print("Next steps:")
print("  1. Review the domain mapping above")
print("  2. Edit docs/phase_outputs/phase1_data_inventory.md")
print("     Fill in 'Confirmed?' and 'Notes / Join Logic' columns")
print("  3. git add docs/phase_outputs/phase1_data_inventory.md")
print("  4. git commit -m 'data: snowflake discovery PRD_MDP.MDP_DSP'")
print("  5. git push origin main")
print("  6. Tell the agent: 'discovery done, inventory committed'")
