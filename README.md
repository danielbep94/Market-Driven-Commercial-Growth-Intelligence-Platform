# Market Growth Intelligence Platform

> **Version 2.3** | Commercial Growth Analytics & ML Decision Engine

A fully integrated analytics and machine learning platform connecting every commercial signal — sell-in, sell-out, waste, investment, forecast, and market data — into a single intelligence layer.

## What this platform does

| Output | Description |
|--------|-------------|
| 📊 Brand Growth Quality Score | Composite signal of sell-out, share, waste, forecast accuracy, and ROI |
| 🔮 Demand Forecast | LightGBM + Prophet per channel (separate models per channel) |
| ⚠️ Waste Risk Engine | Binary classifier — predicts waste exceedance 4 weeks ahead |
| 💰 Investment Efficiency (MMM) | Hill function + adstock decay; marginal return per peso by brand |
| 🏆 Competitive Risk Score | Rule-based composite of share/price/distribution signals (v1) |
| 🎯 Recommendation Engine | Compound-action output (e.g., "INCREASE + WASTE_REVIEW") with rationale |

## Architecture

```
Snowflake (Source) → Databricks (Bronze/Silver/Gold) → Snowflake (MART) → Power BI
```

- **Bronze:** Append-only raw ingestion; schema validated against data contracts
- **Silver:** Homologated, enriched, sell-out method flagged, stock_type classified
- **Gold:** Dimensional model (star schema); KPIs computed; features engineered
- **MART:** Aggregated intelligence tables read by Power BI (Import mode, Monday refresh)

## Scope lock

See [implementation_plan.md](./.gemini/antigravity/) for the full scope lock document (v2.3).
Do not build features, tables, or models not listed in the plan without a formal scope change.

## Key decisions (Phase 0 blockers)

1. **Sell-out definition** — one measurement method per channel, signed off by commercial leadership → `docs/governance/sellout_definition_decision_table.md`
2. **MMM counterfactual** — adstock decay method confirmed with Finance + Trade Marketing → `configs/model_config.yaml`
3. **GQS weights** — signed off by commercial leadership before Phase 4 → `configs/kpi_weights.yaml`

## Folder structure

| Folder | Purpose |
|--------|---------|
| `configs/` | All configuration — thresholds, weights, pipeline parameters |
| `docs/` | Architecture, data contracts, model cards, runbooks, phase outputs |
| `homologation/` | Brand/channel/customer/SKU name mapping dictionaries |
| `notebooks/` | Databricks notebooks by phase (assessment → bronze → silver → gold → kpi → ml) |
| `sql/` | DDL, KPI functions (versioned), views |
| `models/` | Trained model artifacts + rule configs |
| `powerbi/` | Dashboard themes, page specs, RLS matrix |
| `tests/` | Unit, integration, and data quality tests |
| `scripts/` | Setup and validation scripts |

## Configuration files

| File | What it controls |
|------|----------------|
| `configs/pipeline_config.yaml` | Fiscal calendar, Nielsen cadence, ERP policy, forecast version |
| `configs/dq_thresholds.yaml` | Nielsen lag warning (6 weeks), GQS IQR minimum (15 pts), orphan SLA |
| `configs/kpi_weights.yaml` | Versioned GQS component weights |
| `configs/model_config.yaml` | All ML model parameters — adstock bounds, Hill function, retraining triggers |
| `models/recommendation_engine/rules_config.yaml` | Recommendation rules + rationale templates (version controlled) |

## Running locally (development)

All notebook development happens in Databricks (DEV environment).
No direct ERP connections — always use the data warehouse extract.
See `scripts/setup_databricks_secrets.sh` for credential setup.

## Contributing

1. Branch from `main`: `git checkout -b feature/your-feature`
2. Make changes
3. Run unit tests: `pytest tests/unit/`
4. Open PR → requires 1 reviewer approval
5. Merge to `main` triggers CI pipeline

## Team

| Role | Responsibility |
|------|---------------|
| Data Engineering | Bronze/Silver/Gold pipelines, data contracts, schema governance |
| Data Science | KPI engineering, ML model development, model monitoring |
| BI Developer | Power BI dashboard, Snowflake views, RLS configuration |
| Data Steward | Homologation dictionary, FACT_SELL_OUT_UNRESOLVED resolution |
| Commercial Lead | KPI formula sign-off, recommendation engine rules, sell-out definition |
| Finance | Fiscal calendar, MMM counterfactual method |
| Market Intelligence | Nielsen cadence, competitive risk feature validation |
