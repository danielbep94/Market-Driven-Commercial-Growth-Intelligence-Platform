# Channel Hierarchy Standardization Audit

Date: 2026-06-30

## Scope

Audit objective: verify whether the repository implements the target enterprise channel hierarchy:

- `L0 GRAN_CANAL_GRP`
- `L1 CHANNEL_STANDARD`
- `L2 CHAIN_STANDARD`
- `L3 FORMAT_STANDARD`
- Waste-only `BUSINESS_ROUTE`

Sources reviewed:

- `configs/catalog_seeds/canal_unified_seed.csv`
- `notebooks/master_catalog/build_cat_canal.py`
- `notebooks/phase3_silver/silver_sell_in.py`
- `notebooks/phase3_silver/silver_sell_out.py`
- `notebooks/phase3_silver/silver_nielsen.py`
- `SEMANTIC_LAYOUTS/*_MDM.txt`
- `logs/*standardization*`
- `logs/signoff_03_nielsen_markets.csv`
- `logs/signoff_05_store_chain_classification.csv`
- `logs/signoff_05_store_format_classification.csv`

Note: the DBFS path `dbfs:/mnt/mdp/mdm/master_catalog/canal/cat_canal.csv` was not mounted in this local workspace at audit time, so validation used the repo seed, repo log copies, and the catalog build documentation as the nearest committed evidence.

## Audit Summary

Overall status: `FAIL`

Component status:

- Semantic layer: `FAIL`
- MDM / governed catalog: `FAIL`
- Execution logs: `FAIL`
- Waste implementation: `FAIL`

Why it fails:

1. The current catalog is built around legacy levels like `L1_GRAN_CANAL`, `L1_FORMAT`, and `L2_CHAIN`, not the target enterprise levels `CHANNEL_STANDARD`, `CHAIN_STANDARD`, and `FORMAT_STANDARD`.
2. The current promoted catalog still centers on `UTT / DTT / CAM`, not `UTT / DTT / INTERNAL`.
3. `BUSINESS_ROUTE` for Waste is not implemented anywhere in the committed codebase.
4. `SELL_IN`, `SELL_OUT`, and `NIELSEN` each standardize to different channel vocabularies, so the enterprise hierarchy is not consistently applied cross-source.
5. The files that should operationalize Waste and channel gold models are placeholders:
   - `notebooks/phase3_silver/silver_waste.py`
   - `notebooks/phase3_gold/gold_fact_waste.py`
   - `notebooks/phase3_gold/gold_dim_channel.py`

## Evidence

### 1. Current master catalog is a different model

- `build_cat_canal.py` only reads seed columns `canal_raw`, `canal_level`, `gran_canal_grp`, `canal_type`, `promoted`, `confirmation_status`, `notes`.
- The seed schema has no `CHANNEL_STANDARD`, `CHAIN_STANDARD`, `FORMAT_STANDARD`, or `BUSINESS_ROUTE`.
- The promoted catalog is explicitly filtered to `UTT`, `DTT`, `CAM`.

Relevant evidence:

- `notebooks/master_catalog/build_cat_canal.py:153-164`
- `notebooks/master_catalog/build_cat_canal.py:257-299`
- `notebooks/master_catalog/build_cat_canal.py:723-790`
- `notebooks/master_catalog/build_cat_canal.py:1076-1080`
- `docs/cat_canal_build_documentation.md:81-89`
- `docs/cat_canal_build_documentation.md:104-108`
- `docs/cat_canal_build_documentation.md:184-206`

### 2. Sell-in is not mapped to target L1/L2/L3

Current behavior:

- `cadena_std` is intentionally null for all `SELL_IN` rows.
- `canal_std` is copied directly from raw `CUS_GRN_CHL_DSC`.
- Observed values in `logs/sell_in_std.csv`: `DTT`, `UTT`, `NA`, and blank.

Impact:

- `CHANNEL_STANDARD` allowed values `MODERNO / TRADICIONAL / INTERNOS` are not implemented.
- `CHAIN_STANDARD` and `FORMAT_STANDARD` are absent.

Relevant evidence:

- `notebooks/phase3_silver/silver_sell_in.py:191-219`
- `configs/phase3_architectural_decisions.yaml:16-45`
- `logs/phase3_mapping_coverage_report.txt:1-3`

### 3. Sell-out maps to a format-type taxonomy, not the enterprise hierarchy

Current behavior:

- `cadena_std` comes from `logs/signoff_05_store_chain_classification.csv`.
- `canal_std` comes from `logs/signoff_05_store_format_classification.csv`.
- Current `canal_std` vocabulary is:
  - `AUTOSERVICIO`
  - `CEDIS`
  - `CONVENIENCIA`
  - `ECOMMERCE`
  - `MAYOREO`
  - `PROXIMIDAD`

Impact:

- `CHANNEL_STANDARD` is not `MODERNO / TRADICIONAL / INTERNOS`.
- `CHAIN_STANDARD` is banner-level (`WALMART`, `SORIANA`, `HEB`) instead of enterprise group-level (`GRUPO WALMART`, `GRUPO SORIANA`, `GRUPO HEB`).
- `FORMAT_STANDARD` exists implicitly as raw `FORMAT`, but it is modeled as `canal_std` or `L1_FORMAT`, not as level `L3`.

Relevant evidence:

- `notebooks/phase3_silver/silver_sell_out.py:233-273`
- `SEMANTIC_LAYOUTS/SELL_OUT/SELL_OUT_MDM.txt:79-82`
- `logs/phase3_mapping_coverage_report.txt:4-6`

Examples of incorrect parent-child behavior versus the target rules:

- `WM-BOD AURRERA EXPRESS` is mapped to `canal_std = PROXIMIDAD`; target should be `CHANNEL_STANDARD = MODERNO`, `CHAIN_STANDARD = GRUPO WALMART`.
- `SO-EXPRESS` is mapped to `canal_std = PROXIMIDAD`; target should be `CHANNEL_STANDARD = MODERNO`, `CHAIN_STANDARD = GRUPO SORIANA`.
- `HEB-MI TIENDA` is mapped to `canal_std = PROXIMIDAD`; target should be `CHANNEL_STANDARD = MODERNO`, `CHAIN_STANDARD = GRUPO HEB`.
- `HEB-CEDIS` is mapped to `canal_std = CEDIS`; target examples place `HEB CEDIS` under the retail hierarchy, not a separate channel class.
- `CH-SUPERCITO` is mapped to `canal_std = PROXIMIDAD`; target examples place it under `GRUPO CHEDRAUI` in `MODERNO`.

### 4. Nielsen is standardized to market archetypes, not enterprise channels

Current behavior:

- `signoff_03_nielsen_markets.csv` populates `canal_std` with 14 distinct values such as:
  - `AUTOSERVICIO`
  - `AUTOSERVICIO_SCANNING`
  - `AUTOSERVICIO_SURTIDO_COMPLETO`
  - `MODERNO_TOTAL`
  - `TRADICIONAL`
  - `TOTAL_MERCADO`

Impact:

- Nielsen does not land on `CHANNEL_STANDARD`.
- No `CHAIN_STANDARD` or `FORMAT_STANDARD` is modeled for Nielsen.
- `TOTAL_MERCADO` is not an allowed enterprise channel value.

Relevant evidence:

- `notebooks/phase3_silver/silver_nielsen.py:153-206`
- `logs/signoff_03_nielsen_markets.csv`
- `logs/phase3_mapping_coverage_report.txt:11-12`

### 5. Waste is not implemented for the target hierarchy

Current behavior:

- Waste semantic layout still exposes native `CADENA`, `FORMATO`, `CANAL`.
- No committed notebook implements Waste standardization.
- No committed file contains `BUSINESS_ROUTE`.
- Profiling logs show only `NC MODERNO` in the repo copy used for catalog profiling, which means the committed catalog logic does not currently prove the required four-value Waste mapping.

Impact:

- The required Waste mapping:
  - `MODERNO -> MODERNO / COMMERCIAL`
  - `NC MODERNO -> MODERNO / COMMERCIAL`
  - `TRADICIONAL -> TRADICIONAL / COMMERCIAL`
  - `INTERNOS -> INTERNOS / INTERNAL_OPERATION`
  is not implemented end to end.

Relevant evidence:

- `SEMANTIC_LAYOUTS/WASTE/WASTE_MDM.txt:7-23`
- `notebooks/phase3_silver/silver_waste.py:1`
- `notebooks/phase3_gold/gold_fact_waste.py:1`
- `notebooks/phase3_gold/gold_dim_channel.py:1`
- `logs/catalog_profiling/waste_canal_profile.csv`

### 6. The current logs are green, but only for the legacy model

The phase 3 and phase 4 logs show all-pass coverage, but the validations only confirm the old fields are non-null.

They do not validate:

- allowed values for `CHANNEL_STANDARD`
- required parent-child hierarchy across L0-L3
- Waste `BUSINESS_ROUTE`
- cross-source conformance to a single enterprise hierarchy

Relevant evidence:

- `logs/phase3_mapping_coverage_report.txt:1-12`
- `logs/phase3_standardization_audit_log.txt:40-60`
- `logs/phase4_standardization_audit_log.txt:439-443`

## Discrepancy Report

### Missing fields

- Missing everywhere in the committed implementation:
  - `CHANNEL_STANDARD`
  - `CHAIN_STANDARD`
  - `FORMAT_STANDARD`
  - `BUSINESS_ROUTE`

### Invalid L0 vocabulary

- Present:
  - `UTT`
  - `DTT`
  - `CAM`
  - `UNKNOWN`
- Required:
  - `UTT`
  - `DTT`
  - `INTERNAL`

### Invalid L1 vocabulary

- `SELL_IN` uses `UTT / DTT / NA / ''`
- `SELL_OUT` uses `AUTOSERVICIO / PROXIMIDAD / CONVENIENCIA / MAYOREO / ECOMMERCE / CEDIS`
- `NIELSEN` uses 14 market-channel variants
- Required allowed set:
  - `MODERNO`
  - `TRADICIONAL`
  - `INTERNOS`

### Missing or mis-modeled L2 groups

The following required enterprise chain groups are absent from the current chain mapping file:

- `GRUPO WALMART`
- `GRUPO SORIANA`
- `GRUPO CHEDRAUI`
- `GRUPO HEB`
- `GRUPO SAMS`
- `GRUPO CASA LEY`
- `GRUPO OXXO`
- `GRUPO AL SUPER`
- `GRUPO FARMACIAS GUADALAJARA`

### Missing or mis-modeled L3 values

The current committed catalog does not model the required enterprise `FORMAT_STANDARD` layer. Missing examples include:

- `CADENA WALMART`
- `CADENA BODEGA AURRERA`
- `CADENA MI BODEGA AURRERA`
- `CADENA SUPERCENTER`
- `HEB MI TIENDA`
- `HEB CEDIS`
- `0049 MX PLANTA IPP`
- `0049 MX XD IRAPUATO`
- `0049 MX FDC IRAPUATO`
- `0049 MX CEDyS IRAPUATO`
- `0049 MX TLAHUAC`
- `0049 MX MERIDA`

### Parent-child problems

- `FORMAT` is promoted as `L1_FORMAT`, not as `L3 FORMAT_STANDARD`.
- `CHAIN` is stored as `L2_CHAIN` reference only and explicitly marked `REFERENCE_ONLY`, so it cannot serve as canonical `CHAIN_STANDARD`.
- `SELL_IN` has no enterprise chain level at all in the current runtime model.
- `IBP CADENA` and `SELL_OUT CHAIN` only match at 21.9%, which confirms there is no reconciled chain bridge today.

### Orphan / unsupported values

- `CAM` is still promoted but is not part of the requested `UTT / DTT / INTERNAL` enterprise model.
- `NA` and blank `SELL_IN` channels remain in the standardized output.
- `TOTAL_MERCADO` and similar Nielsen buckets remain in `canal_std`, but are outside the target hierarchy.

### Business-rule conflict to resolve before production sign-off

- OXXO is inconsistent in the supplied rules:
  - Section `L0 GRAN_CANAL_GRP` describes OXXO as a `DTT` example.
  - Section `L2 CHAIN_STANDARD` lists `GRUPO OXXO` under `MODERNO`.
- The current repository does not implement either target interpretation cleanly:
  - the master catalog places OXXO-related sell-out formats under `DTT`-style legacy logic
  - the sell-out runtime maps `OXXO` to `CONVENIENCIA`
- Final production mapping for OXXO should be explicitly approved before the enterprise hierarchy is promoted.

## Exact Fixes

### 1. Replace the legacy seed with an enterprise seed

Use a governed seed with the target shape:

```csv
source_system,source_column,source_value,gran_canal_grp,channel_standard,chain_standard,format_standard,business_route,mapping_status,notes
```

Starter template added in:

- `configs/catalog_seeds/channel_hierarchy_standardization_seed_template.csv`

### 2. Build the enterprise dimension from that seed

Example dbt model:

```sql
with seed as (
    select
        upper(trim(source_system))       as source_system,
        upper(trim(source_column))       as source_column,
        upper(trim(source_value))        as source_value,
        upper(trim(gran_canal_grp))      as gran_canal_grp,
        upper(trim(channel_standard))    as channel_standard,
        upper(trim(chain_standard))      as chain_standard,
        upper(trim(format_standard))     as format_standard,
        upper(trim(business_route))      as business_route,
        upper(trim(mapping_status))      as mapping_status,
        notes
    from {{ ref('channel_hierarchy_standardization_seed') }}
),
validated as (
    select *
    from seed
    where mapping_status = 'CONFIRMED'
      and gran_canal_grp in ('UTT', 'DTT', 'INTERNAL')
      and channel_standard in ('MODERNO', 'TRADICIONAL', 'INTERNOS')
)
select
    {{ dbt_utils.generate_surrogate_key([
        'source_system',
        'source_column',
        'source_value',
        'gran_canal_grp',
        'channel_standard',
        'chain_standard',
        'format_standard'
    ]) }} as channel_key,
    source_system,
    source_column,
    source_value,
    gran_canal_grp,
    channel_standard,
    chain_standard,
    format_standard,
    business_route,
    notes
from validated
```

### 3. Fix Waste explicitly

Example SQL for Waste staging:

```sql
with waste_src as (
    select
        fecha,
        sku,
        upper(trim(cadena))  as cadena_raw,
        upper(trim(formato)) as formato_raw,
        upper(trim(canal))   as canal_raw,
        upper(trim(fuente))  as fuente,
        waste_amount,
        waste_kg
    from {{ source('mdp_stg', 'vw_waste') }}
    where upper(trim(fuente)) = 'TOPLINE'
),
waste_mapped as (
    select
        w.*,
        case
            when canal_raw in ('MODERNO', 'NC MODERNO') then 'UTT'
            when canal_raw = 'TRADICIONAL' then 'DTT'
            when canal_raw = 'INTERNOS' then 'INTERNAL'
            else null
        end as gran_canal_grp,
        case
            when canal_raw in ('MODERNO', 'NC MODERNO') then 'MODERNO'
            when canal_raw = 'TRADICIONAL' then 'TRADICIONAL'
            when canal_raw = 'INTERNOS' then 'INTERNOS'
            else null
        end as channel_standard,
        case
            when canal_raw in ('MODERNO', 'NC MODERNO', 'TRADICIONAL') then 'COMMERCIAL'
            when canal_raw = 'INTERNOS' then 'INTERNAL_OPERATION'
            else null
        end as business_route
    from waste_src w
)
select *
from waste_mapped
```

### 4. Stop copying raw values into `canal_std`

Current anti-patterns to remove:

- `silver_sell_in.py` copies `canal_raw` directly into `canal_std`
- `silver_sell_out.py` populates `canal_std` from format type classes
- `silver_nielsen.py` writes market-taxonomy values to `canal_std`

Recommended replacement in Python:

```python
CHANNEL_STANDARD_MAP = {
    "UTT": "MODERNO",
    "DTT": "TRADICIONAL",
    "INTERNAL": "INTERNOS",
    "MODERNO": "MODERNO",
    "NC MODERNO": "MODERNO",
    "TRADICIONAL": "TRADICIONAL",
    "INTERNOS": "INTERNOS",
}

GRAN_CANAL_MAP = {
    "MODERNO": "UTT",
    "TRADICIONAL": "DTT",
    "INTERNOS": "INTERNAL",
}
```

Then derive canonical columns explicitly:

```python
df = (
    df
    .withColumn("channel_standard", udf_channel_standard(F.col("source_value")))
    .withColumn("gran_canal_grp", udf_gran_canal(F.col("channel_standard")))
    .withColumn("chain_standard", udf_chain_standard(F.col("source_value"), F.col("source_system")))
    .withColumn("format_standard", udf_format_standard(F.col("source_value"), F.col("source_system")))
)
```

### 5. Add validation gates that actually test the target model

Example assertions:

```python
assert set(df.select("channel_standard").distinct().toPandas()["channel_standard"]) <= {
    "MODERNO", "TRADICIONAL", "INTERNOS"
}

assert set(df.select("gran_canal_grp").distinct().toPandas()["gran_canal_grp"]) <= {
    "UTT", "DTT", "INTERNAL"
}

assert df.filter(
    (F.col("channel_standard") == "MODERNO") &
    F.col("chain_standard").isNull()
).count() == 0

assert df.filter(
    (F.col("source_system") == "WASTE") &
    F.col("business_route").isNull()
).count() == 0
```

### 6. Fill the empty implementation files before promoting the model

The following files are placeholders and should not be treated as production implementation:

- `notebooks/phase3_silver/silver_waste.py`
- `notebooks/phase3_gold/gold_fact_waste.py`
- `notebooks/phase3_gold/gold_dim_channel.py`
- `sql/ddl/dimensions/dim_channel.sql`
- `sql/ddl/facts/fact_waste.sql`

## Recommended next step

Implement the enterprise hierarchy as a new governed seed and use it to drive:

1. Waste staging standardization
2. Sell-in enterprise channel normalization
3. Sell-out chain and format normalization
4. Nielsen channel normalization
5. A new `dim_channel` built from the target L0-L3 structure

Only after those are in place should the runtime logs be treated as evidence of successful enterprise standardization.
