# Model Card — Demand Forecast Engine

**Version:** [TBD upon first training]  
**Status:** DESIGN PHASE  
**MLflow registry name:** `demand_forecast_lgbm`  
**Owner:** Data Scientist  
**Last updated:** [DATE]

---

## Business Output
Weekly demand forecast at SKU × Channel level, 4–13 week horizon.
Enables proactive production planning and prevents stockouts and overstock.

## Model Specification

| Attribute | Value |
|-----------|-------|
| Algorithm | LightGBM (primary); Prophet (baseline comparison) |
| Target variable | `sell_out_units` (NOT sell-in) |
| Forecast horizon | 4–13 weeks |
| Training frequency | As-triggered (see retraining triggers below) |
| Channels | Trained SEPARATELY per channel (Modern Trade, Traditional Trade, E-commerce, Foodservice) |

## Features Used

| Feature | Type | Description |
|---------|------|-------------|
| `sell_out_units_lag4w` | Numeric | Sell-out 4 weeks ago |
| `sell_out_units_lag8w` | Numeric | Sell-out 8 weeks ago |
| `sell_out_units_lag13w` | Numeric | Sell-out 13 weeks ago |
| `sell_out_rolling_mean_4w` | Numeric | Rolling mean of last 4 weeks sell-out |
| `sell_out_rolling_mean_13w` | Numeric | Rolling mean of last 13 weeks sell-out |
| `sell_out_yoy_growth` | Numeric | Year-over-year sell-out growth |
| `waste_rate_lag4w` | Numeric | Waste rate 4 weeks ago |
| `investment_spend_lag4w` | Numeric | Investment spend 4 weeks ago |
| `is_holiday` | Binary | 1 if week contains a holiday |
| `weeks_since_promotion` | Numeric | Weeks since last promotion event |
| `is_promotion_active` | Binary | 1 if promotion active this week |
| `price_index_vs_category` | Numeric | Price vs. category average |
| `season` | Categorical | Q1/Q2/Q3/Q4 |
| `sku_age_weeks` | Numeric | Weeks since SKU launch |

## Cold-Start Handling (New SKUs)
- If SKU has < 4 weeks of sell-out history: use brand-level + category-level average as prior
- Flag forecast as "NEW SKU — LIMITED ACCURACY" in output table
- Do NOT train on new SKU until 13 weeks of history is available

## Validation Results

| Metric | Baseline (internal forecast) | This model | Target |
|--------|------------------------------|------------|--------|
| WAPE | [TBD] | [TBD] | 10–20% improvement over baseline |
| Forecast Accuracy (FA) | [TBD] | [TBD] | |
| Bias | [TBD] | [TBD] | Near zero |

Hold-out period: Last 13 weeks of available data.

## Retraining Triggers
1. WAPE degrades more than 5 percentage points from registered baseline
2. New channel or major customer added (structural change)
3. Nielsen methodology changes (competitive signal shift)
4. 13 weeks of new data available since last training date

## Explainability
SHAP feature importance computed for every prediction. Top 3 features surfaced in Power BI Forecast Performance page tooltip.

## Known Limitations
- Traditional trade sell-out coverage may be < 70% in some regions — forecast accuracy for those segments will be flagged as LOW confidence
- Holiday weeks with extreme spikes require the `is_holiday` feature; if holiday calendar is incomplete, spikes will cause systematic over-forecast the following week
- Model does not capture new product launches until 13 weeks of history is available

## Approval Log

| Action | Person | Date | Notes |
|--------|--------|------|-------|
| Model design reviewed | [TBD] | [TBD] | |
| Baseline comparison completed | [TBD] | [TBD] | |
| Production deployment approved | [TBD] | [TBD] | |
