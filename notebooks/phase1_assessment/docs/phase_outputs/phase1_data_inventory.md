# Phase 1 — Source Discovery & Validation

**Generated:** 2026-06-05 01:42 UTC  |  **Snowflake:** `danonenam.east-us-2.azure.snowflakecomputing.com`  |  **Warehouse:** `PRD_MDP_ANL_WH`  |  **Role:** `PRD_MDP`
**Sources profiled:** 2/10  |  **Pending:** 8/10

## Scorecard

| Source | Domain | Rows | Score | Status |
|--------|--------|------|-------|--------|
| `DATA_MKT` | Investment / Marketing | 46,642 | 50/100 | 🔴 NOT READY |
| `DATA_WASTE` | Waste / Merma | 7,099,255 | 65/100 | 🟡 CONDITIONAL |

## Pending Sources

| Source | Domain | Status |
|--------|--------|--------|
| `DATA_SELL_IN` | Sell-In | SQL not yet defined |
| `DATA_SELL_OUT` | Sell-Out | SQL not yet defined |
| `DATA_FORECAST` | Demand Forecast | SQL not yet defined |
| `DATA_NIELSEN` | Nielsen / Market Share | SQL not yet defined |
| `DATA_PRICE` | Price | SQL not yet defined |
| `DATA_PROMO` | Promotions | SQL not yet defined |
| `DATA_INVENTORY` | Inventory / Stock | SQL not yet defined |
| `DATA_CALENDAR` | Calendar / Date Dimension | SQL not yet defined |


---

## DATA_MKT — Investment / Marketing

| | |
|--|--|
| **Readiness** | 50/100 🔴 NOT READY |
| **Database** | `PRD_MDP` |
| **Schema** | `MDP_DSP` |
| **Rows** | 46,642 |
| **Columns** | 77 |
| **Date column** | `ANIO` |
| **Date range** | 2024.0 → 2026.0 (3 periods) |
| **Duplicates** | 46,562 (99.83%) |

**SQL:**
```sql
SELECT * FROM PRD_MDP.MDP_DSP.VW_MKT_ECOMM WHERE anio >= 2024
```

### Schema
| Column | Type | Nullable |
|--------|------|---------|
| `MEDIO` | `StringType()` | ✓ |
| `SOPORTE_PLATAFORMA` | `StringType()` | ✓ |
| `CATEGORIA` | `StringType()` | ✓ |
| `MARCA` | `StringType()` | ✓ |
| `ANUNCIANTE` | `StringType()` | ✓ |
| `TARGET` | `StringType()` | ✓ |
| `DIVISA` | `StringType()` | ✓ |
| `TRIBU` | `StringType()` | ✓ |
| `FUENTE` | `StringType()` | ✓ |
| `ECOMM` | `StringType()` | ✓ |
| `CAMPANA` | `StringType()` | ✓ |
| `ANIO` | `DoubleType()` | ✓ |
| `FECHA` | `DateType()` | ✓ |
| `NUM_MES` | `DoubleType()` | ✓ |
| `PERIODO` | `StringType()` | ✓ |
| `LINE_ITEM` | `StringType()` | ✓ |
| `FECHA_LINE_ITEM` | `StringType()` | ✓ |
| `LINE_ITEM_TYPE` | `StringType()` | ✓ |
| `IMPRESIONES` | `DoubleType()` | ✓ |
| `ACTIVE_VIEW` | `DecimalType(38,0)` | ✓ |
| `CLICS` | `DoubleType()` | ✓ |
| `VIDEO_50` | `DoubleType()` | ✓ |
| `VIDEO_100` | `DoubleType()` | ✓ |
| `VISTAS` | `DoubleType()` | ✓ |
| `INVERSION_REAL` | `DoubleType()` | ✓ |
| `ALCANCE` | `DecimalType(38,0)` | ✓ |
| `IDENTIFICADOR` | `StringType()` | ✓ |
| `OBJETIVO` | `StringType()` | ✓ |
| `TIPO_COMPRA` | `StringType()` | ✓ |
| `OBJETIVO_FUNNEL` | `StringType()` | ✓ |
| `PROSPECTING_REMARKETING` | `StringType()` | ✓ |
| `FORMATO` | `StringType()` | ✓ |
| `INICIO_INFORME` | `StringType()` | ✓ |
| `FIN_INFORME` | `StringType()` | ✓ |
| `NOMBRE_CUENTA` | `StringType()` | ✓ |
| `NOMBRE_ANUNCIO` | `StringType()` | ✓ |
| `GRUPO_ANUNCIOS` | `StringType()` | ✓ |
| `VIDEO_3SEG` | `DoubleType()` | ✓ |
| `CLICS_ENLACE` | `DoubleType()` | ✓ |
| `THRUPLAY` | `DecimalType(38,0)` | ✓ |
| `INTERACCION` | `DecimalType(38,0)` | ✓ |
| `CONVERSIONES` | `DoubleType()` | ✓ |
| `ID_CLIENTE` | `StringType()` | ✓ |
| `PALABRA_CLAVE` | `StringType()` | ✓ |
| `CONCORDANCIA` | `StringType()` | ✓ |
| `CTR` | `DoubleType()` | ✓ |
| `CPC` | `DoubleType()` | ✓ |
| `CPM` | `DoubleType()` | ✓ |
| `IMPR_ABS_TOP` | `DoubleType()` | ✓ |
| `IMPR_TOP` | `DoubleType()` | ✓ |
| `CONVERSION_PERCENT` | `DoubleType()` | ✓ |
| `VIEW_CONVERSION` | `DoubleType()` | ✓ |
| `COST_CONV` | `DoubleType()` | ✓ |
| `CONV_RATE` | `DoubleType()` | ✓ |
| `VIDEO_25` | `DoubleType()` | ✓ |
| `VIDEO_75` | `DoubleType()` | ✓ |
| `RETAIL` | `StringType()` | ✓ |
| `NUM_SEMANA` | `DecimalType(38,0)` | ✓ |
| `MES` | `StringType()` | ✓ |
| `CLASIFICACION` | `StringType()` | ✓ |
| `CLUSTER` | `StringType()` | ✓ |
| `CADENA` | `StringType()` | ✓ |
| `PLACEMENT_1` | `StringType()` | ✓ |
| `PLACEMENT_2` | `StringType()` | ✓ |
| `DETALLE_CATEGORIA_PRENDIDA_KW_PRENDIDAS` | `StringType()` | ✓ |
| `MENSAJE` | `StringType()` | ✓ |
| `CLAIM_CTA` | `StringType()` | ✓ |
| `PRODUCTOS` | `StringType()` | ✓ |
| `HERO_PRODUCTO` | `StringType()` | ✓ |
| `UPC` | `StringType()` | ✓ |
| `DBAS_Y_N` | `StringType()` | ✓ |
| `VALUE_ATTRIBUTE` | `StringType()` | ✓ |
| `VALUE_TYPE` | `StringType()` | ✓ |
| `CVR` | `DoubleType()` | ✓ |
| `CPI` | `DoubleType()` | ✓ |
| `"IMPR_(ABS.TOP)_%"` | `DoubleType()` | ✓ |
| `"IMPR_(TOP)"` | `DoubleType()` | ✓ |

### Null Rates (non-zero only)
| `SOPORTE_PLATAFORMA` | 10,704 | 22.95% | ⚠️  HIGH |
| `ANUNCIANTE` | 31,496 | 67.53% | ⚠️  HIGH |
| `TARGET` | 31,496 | 67.53% | ⚠️  HIGH |
| `DIVISA` | 5,032 | 10.79% | ⚠️  HIGH |
| `TRIBU` | 46,642 | 100.0% | ⚠️  HIGH |
| `FUENTE` | 17,164 | 36.8% | ⚠️  HIGH |
| `ECOMM` | 31,385 | 67.29% | ⚠️  HIGH |
| `CAMPANA` | 66 | 0.14% | ✅  OK |
| `PERIODO` | 28,033 | 60.1% | ⚠️  HIGH |
| `LINE_ITEM` | 31,496 | 67.53% | ⚠️  HIGH |
| `FECHA_LINE_ITEM` | 36,698 | 78.68% | ⚠️  HIGH |
| `LINE_ITEM_TYPE` | 31,496 | 67.53% | ⚠️  HIGH |
| `IMPRESIONES` | 222 | 0.48% | ✅  OK |
| `ACTIVE_VIEW` | 31,496 | 67.53% | ⚠️  HIGH |
| `CLICS` | 240 | 0.51% | ✅  OK |
| `VIDEO_50` | 10,179 | 21.82% | ⚠️  HIGH |
| `VIDEO_100` | 10,179 | 21.82% | ⚠️  HIGH |
| `VISTAS` | 7,597 | 16.29% | ⚠️  HIGH |
| `INVERSION_REAL` | 1,475 | 3.16% | 🔶 WARN |
| `ALCANCE` | 9,078 | 19.46% | ⚠️  HIGH |
| `OBJETIVO` | 28,597 | 61.31% | ⚠️  HIGH |
| `TIPO_COMPRA` | 28,597 | 61.31% | ⚠️  HIGH |
| `OBJETIVO_FUNNEL` | 28,597 | 61.31% | ⚠️  HIGH |
| `PROSPECTING_REMARKETING` | 46,642 | 100.0% | ⚠️  HIGH |
| `FORMATO` | 26,709 | 57.26% | ⚠️  HIGH |
| `INICIO_INFORME` | 26,789 | 57.44% | ⚠️  HIGH |
| `FIN_INFORME` | 26,789 | 57.44% | ⚠️  HIGH |
| `NOMBRE_CUENTA` | 20,178 | 43.26% | ⚠️  HIGH |
| `NOMBRE_ANUNCIO` | 28,597 | 61.31% | ⚠️  HIGH |
| `GRUPO_ANUNCIOS` | 19,346 | 41.48% | ⚠️  HIGH |
| `VIDEO_3SEG` | 28,881 | 61.92% | ⚠️  HIGH |
| `CLICS_ENLACE` | 28,725 | 61.59% | ⚠️  HIGH |
| `THRUPLAY` | 28,906 | 61.97% | ⚠️  HIGH |
| `INTERACCION` | 14,309 | 30.68% | ⚠️  HIGH |
| `CONVERSIONES` | 16,003 | 34.31% | ⚠️  HIGH |
| `ID_CLIENTE` | 38,223 | 81.95% | ⚠️  HIGH |
| `PALABRA_CLAVE` | 38,228 | 81.96% | ⚠️  HIGH |
| `CONCORDANCIA` | 40,056 | 85.88% | ⚠️  HIGH |
| `CTR` | 35,145 | 75.35% | ⚠️  HIGH |
| `CPC` | 35,311 | 75.71% | ⚠️  HIGH |
| `CPM` | 38,223 | 81.95% | ⚠️  HIGH |
| `IMPR_ABS_TOP` | 42,314 | 90.72% | ⚠️  HIGH |
| `IMPR_TOP` | 42,314 | 90.72% | ⚠️  HIGH |
| `CONVERSION_PERCENT` | 38,632 | 82.83% | ⚠️  HIGH |
| `VIEW_CONVERSION` | 45,645 | 97.86% | ⚠️  HIGH |
| `COST_CONV` | 45,645 | 97.86% | ⚠️  HIGH |
| `CONV_RATE` | 45,645 | 97.86% | ⚠️  HIGH |
| `VIDEO_25` | 39,675 | 85.06% | ⚠️  HIGH |
| `VIDEO_75` | 39,675 | 85.06% | ⚠️  HIGH |
| `RETAIL` | 46,642 | 100.0% | ⚠️  HIGH |
| `NUM_SEMANA` | 3,143 | 6.74% | ⚠️  HIGH |
| `MES` | 3,143 | 6.74% | ⚠️  HIGH |
| `CLUSTER` | 43,499 | 93.26% | ⚠️  HIGH |
| `CADENA` | 43,499 | 93.26% | ⚠️  HIGH |
| `PLACEMENT_1` | 43,499 | 93.26% | ⚠️  HIGH |
| `PLACEMENT_2` | 43,499 | 93.26% | ⚠️  HIGH |
| `DETALLE_CATEGORIA_PRENDIDA_KW_PRENDIDAS` | 43,624 | 93.53% | ⚠️  HIGH |
| `MENSAJE` | 43,520 | 93.31% | ⚠️  HIGH |
| `CLAIM_CTA` | 45,286 | 97.09% | ⚠️  HIGH |
| `PRODUCTOS` | 44,056 | 94.46% | ⚠️  HIGH |
| `HERO_PRODUCTO` | 44,089 | 94.53% | ⚠️  HIGH |
| `UPC` | 44,150 | 94.66% | ⚠️  HIGH |
| `DBAS_Y_N` | 45,290 | 97.1% | ⚠️  HIGH |
| `VALUE_ATTRIBUTE` | 46,298 | 99.26% | ⚠️  HIGH |
| `VALUE_TYPE` | 46,323 | 99.32% | ⚠️  HIGH |
| `CVR` | 45,880 | 98.37% | ⚠️  HIGH |
| `CPI` | 43,668 | 93.62% | ⚠️  HIGH |
| `"IMPR_(ABS.TOP)_%"` | 46,642 | 100.0% | ⚠️  HIGH |
| `"IMPR_(TOP)"` | 46,642 | 100.0% | ⚠️  HIGH |

### Key Field Cardinality
| Column | Distinct |
|--------|---------|
| `CATEGORIA` | 14 |
| `MARCA` | 32 |
| `PROSPECTING_REMARKETING` | 0 |
| `ID_CLIENTE` | 12 |
| `DETALLE_CATEGORIA_PRENDIDA_KW_PRENDIDAS` | 229 |
| `PRODUCTOS` | 725 |
| `HERO_PRODUCTO` | 404 |

### Numeric Volume
| Column | Min | Max | Total | Negatives? |
|--------|-----|-----|-------|-----------|
| `IMPRESIONES` | 0.0 | 85,935,457.0 | 19,336,845,762 | ✅ |
| `INVERSION_REAL` | 0.0 | 2,376,710.9 | 352,836,280 | ✅ |
| `CTR` | 0.0 | 9,977.8 | 5,695,469 | ✅ |
| `CPC` | 0.0 | 197,472.4 | 1,013,793 | ✅ |
| `CPM` | 0.2 | 1,933,721.0 | 49,485,992 | ✅ |

### Open Items — fill in after reviewing
- [ ] Is the date range correct for this source?
- [ ] Are the key cardinalities plausible?
- [ ] What is the business natural key for deduplication?
- [ ] Does this source need a JOIN with another view?
- [ ] Are there any negative numeric values that need explanation?


---

## DATA_WASTE — Waste / Merma

| | |
|--|--|
| **Readiness** | 65/100 🟡 CONDITIONAL |
| **Database** | `PRD_MDP` |
| **Schema** | `MDP_DSP` |
| **Rows** | 7,099,255 |
| **Columns** | 14 |
| **Date column** | `ANIO` |
| **Date range** | 2021 → 2026 (6 periods) |
| **Duplicates** | 7,091,607 (99.89%) |

**SQL:**
```sql
SELECT * FROM PRD_MDP.MDP_STG.VW_WASTE
```

### Schema
| Column | Type | Nullable |
|--------|------|---------|
| `ANIO` | `DecimalType(38,0)` | ✓ |
| `MES` | `DecimalType(38,0)` | ✓ |
| `CADENA` | `StringType()` | ✓ |
| `SKU` | `StringType()` | ✓ |
| `DESCRIPCION` | `StringType()` | ✓ |
| `TIPO_PRODUCTO` | `StringType()` | ✓ |
| `MARCA` | `StringType()` | ✓ |
| `PROYECTO` | `StringType()` | ✓ |
| `CANAL` | `StringType()` | ✓ |
| `"Waste ($)"` | `DoubleType()` | ✓ |
| `"Waste (KG)"` | `DoubleType()` | ✓ |
| `FUENTE` | `StringType()` | ✗ |
| `FECHA` | `DateType()` | ✓ |
| `FORMATO` | `StringType()` | ✓ |

### Null Rates (non-zero only)
| `TIPO_PRODUCTO` | 4,507 | 0.06% | ✅  OK |
| `PROYECTO` | 36,659 | 0.52% | ✅  OK |
| `"Waste ($)"` | 1,383,426 | 19.49% | ⚠️  HIGH |
| `"Waste (KG)"` | 1,385,291 | 19.51% | ⚠️  HIGH |

### Key Field Cardinality
| Column | Distinct |
|--------|---------|
| `SKU` | 1,421 |
| `TIPO_PRODUCTO` | 15 |
| `MARCA` | 27 |
| `CANAL` | 4 |

### Numeric Volume
| Column | Min | Max | Total | Negatives? |
|--------|-----|-----|-------|-----------|
| `"Waste ($)"` | -18,866,272.3 | 15,596,809.2 | 2,992,742,016 | ⚠️ |
| `"Waste (KG)"` | -646,512.3 | 212,288.5 | 65,365,578 | ⚠️ |

### Open Items — fill in after reviewing
- [ ] Is the date range correct for this source?
- [ ] Are the key cardinalities plausible?
- [ ] What is the business natural key for deduplication?
- [ ] Does this source need a JOIN with another view?
- [ ] Are there any negative numeric values that need explanation?



---

## Next Step

1. Fill in **Open Items** above for each source
2. Add SQL for pending sources and re-run
3. `git add docs/phase_outputs/phase1_data_inventory.md`
4. `git commit -m "data: source discovery 2/10 sources profiled"`
5. `git push origin main`
6. Tell the agent: **"discovery done, inventory committed"**
