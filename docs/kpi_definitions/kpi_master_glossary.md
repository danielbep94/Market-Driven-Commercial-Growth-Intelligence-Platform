# KPI Master Glossary

**Status:** DRAFT — all formulas must be approved by a business stakeholder before Phase 4 implementation.  
**Stakeholder sign-off required:** [NAME] | [DATE]

---

## KPI Definitions

### 1. Sell-out / Sell-in Ratio
- **Formula:** `sell_out_units / sell_in_units`
- **Grain:** Brand × Channel × Week
- **Business question:** Is the market pulling product, or are we pushing inventory?
- **Target direction:** Higher → healthier (closer to 1.0 = balanced; >1 = strong pull)
- **Edge cases:**
  - `sell_in_units = 0` → Return NULL (do not divide; exclude from rolling average)
  - `sell_out_units > sell_in_units` → Valid; ratio > 1.0 means sales from prior inventory. Allow; cap dashboard display at 1.5x with overflow label.
- **Confidence flag:** LOW if sell-out coverage < 70% for that channel

### 2. Waste Rate
- **Formula:** `waste_units / sell_in_units`
- **Grain:** Brand × Customer × Week
- **Business question:** What share of what we ship comes back as waste?
- **Target direction:** Lower → healthier
- **Edge cases:**
  - `sell_in_units = 0` → Return NULL
  - `waste_units > sell_in_units` → Flag as anomaly in DQ_LOG; exclude from waste_rate KPI pending validation
- **Threshold:** [TO BE DEFINED by business — e.g., > 5% = high risk]

### 3. Forecast Bias
- **Formula:** `MEAN((forecast_units - actual_units) / actual_units)` over trailing N weeks
- **Grain:** Brand × Channel × Week
- **Business question:** Are we systematically over- or under-forecasting?
- **Target direction:** Near zero (positive = over-forecast; negative = under-forecast)
- **Edge cases:**
  - `actual_units = 0` → Exclude period from bias calculation; log as "zero-actual event"
  - Sustained bias (3+ consecutive periods same direction) → trigger "consecutive miss flag"

### 4. Forecast Accuracy (WAPE)
- **Formula:** `SUM(ABS(forecast_units - actual_units)) / SUM(actual_units)`
- **Grain:** Brand × Channel × rolling 13 weeks
- **Business question:** How accurate is our forecast overall?
- **Target direction:** Lower → better (0% = perfect)
- **Edge cases:**
  - `SUM(actual_units) = 0` → Return NULL; cannot compute WAPE
  - New SKUs with < 4 weeks of actuals → flag as "insufficient history"

### 5. Waste-Adjusted ROI
- **Formula:** `(sell_out_revenue - waste_cost) / investment_spend`
- **Grain:** Brand × Channel × Month
- **Business question:** What is the real return after accounting for waste losses?
- **Target direction:** Higher → better (>1.0 = positive return)
- **Edge cases:**
  - `investment_spend = 0` → Return NULL; flag brand as "unsponsored" in dashboard
  - `(sell_out_revenue - waste_cost) < 0` → Allow negative ROI; flag in Waste Risk page
  - `waste_cost > sell_out_revenue` → Flag as "catastrophic loss" alert

### 6. Market-Adjusted Growth
- **Formula:** `brand_sell_out_growth_pct - category_market_growth_pct`
- **Grain:** Brand × Channel × Period (aligned with Nielsen frequency)
- **Business question:** Are we growing because the category is growing, or despite it?
- **Target direction:** Positive → outperforming category
- **Edge cases:**
  - Nielsen data unavailable for period → Return NULL; exclude brand from ranking
  - Nielsen lag: align internal data to Nielsen period (e.g., lag internal data by N weeks)

### 7. Share Change
- **Formula:** `current_period_share - prior_period_share` (from Nielsen)
- **Grain:** Brand × Category × Channel × Period
- **Business question:** Are we gaining or losing category position?
- **Target direction:** Positive → gaining share
- **Edge cases:**
  - New brand (no prior period) → Return NULL for share change; flag as "new entrant"
  - Nielsen methodology change → flag comparison break; do not compute share change across break

### 8. Growth Quality Score (GQS)
- **Formula:** Composite index (weights TBD by commercial leadership)
  ```
  GQS = w1 × sell_out_growth_score
      + w2 × share_change_score
      + w3 × waste_rate_score (inverted)
      + w4 × forecast_accuracy_score (inverted WAPE)
      + w5 × waste_adjusted_roi_score
  ```
  Proposed weights: w1=0.25, w2=0.25, w3=0.20, w4=0.15, w5=0.15
- **Scale:** 0–100
- **Target direction:** Higher → healthier
- **Edge cases:**
  - Brand with < 4 weeks of history → Suppress score; show raw metrics only; flag "insufficient history"
  - Missing Nielsen data → Redistribute share_change weight proportionally to other components; flag partial score
  - Any component score NULL → Recalculate weights excluding NULL components; flag partial score

### 9. Waste Risk Score (ML-derived)
- **Formula:** Probability output (0–1) from Waste Risk Engine (LightGBM classifier)
- **Categorization:** Low (<0.3), Medium (0.3–0.7), High (>0.7)
- **Business question:** What is the probability of waste exceeding threshold in the next 4 weeks?
- **Target direction:** Lower → safer
- **Edge cases:**
  - Brand/customer with no historical waste data → Flag "limited waste history"; use cross-customer prior

### 10. Competitive Risk Score (ML-derived)
- **Formula:** Score 0–100 derived from unsupervised clustering + rate of share loss + price gap widening
- **Alert threshold:** Score > 70 for 2 consecutive periods → automatic flag in dashboard
- **Business question:** How vulnerable is this brand to competitive pressure?
- **Target direction:** Lower → safer
- **Edge cases:**
  - Single brand in category (no competition) → Return NULL; flag "no competitive context"

---

## Approval Log

| KPI | Reviewer | Date | Decision | Notes |
|-----|---------|------|---------|-------|
| All KPIs | [TBD] | [TBD] | PENDING | Weights for GQS require separate sign-off |
