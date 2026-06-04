# Model Card — Investment Efficiency Engine (MMM)

**Version:** [TBD upon first training]
**Status:** DESIGN PHASE — awaiting Phase 5 blocker sign-off
**MLflow registry name:** `investment_efficiency_mmm_hill_adstock`
**Owner:** Data Scientist
**Business co-owner:** Finance + Trade Marketing

> [!IMPORTANT]
> Three decisions below must be confirmed with Finance and Trade Marketing
> **before `02_adstock_transform.py` or `03_mmm_regression.py` are built.**

---

## Business Output
Estimated incremental sell-out units per unit of measured investment, by brand × channel × month.
Identifies diminishing returns thresholds. Enables Page 5 (Investment Efficiency) dashboard.

---

## Target Variable — CONFIRMED REQUIRED BEFORE BUILD

**Target:** `incremental_sell_out_units`

Definition: observed `sell_out_units` minus the counterfactual — what sell-out would have been in the absence of investment.

This is NOT total sell-out. It is the lift attributable to measured investment above the no-investment baseline.

---

## Counterfactual Method — CONFIRMED REQUIRED BEFORE BUILD

**Method: Adstock Decay Model (recommended for v1)**

Adstock models the carry-over effect of investment across weeks:

```
adstock_t = spend_t + decay_rate × adstock_(t-1)
```

Where:
- `spend_t` = measured investment in week t
- `decay_rate` ∈ [0.1, 0.9] = fraction of prior week's effect carried forward
  - 0.3 = fast-fading effect (e.g., price promotions)
  - 0.8 = long-lasting effect (e.g., brand advertising)
- `adstock_t` = "effective investment" used as input to the Hill function

`decay_rate` is **estimated per brand × channel** from the data during Phase 5 EDA.
Estimated values are stored in `MODEL_METADATA` and `models/investment_efficiency/adstock_params.json`.

**Why adstock over holdout or synthetic control:**
- No holdout group available (investment covers all accounts in most periods)
- Synthetic control requires matched untreated units (not feasible for most brand portfolios)
- Adstock is the CPG industry standard for MMM with continuous investment data
- Produces the diminishing returns curve needed for Page 5 visualization

---

## Saturation Curve — CONFIRMED REQUIRED BEFORE BUILD

**Curve type: Hill Function**

```
response(x) = x^α / (K^α + x^α)
```

Where:
- `x` = `adstock_t` (effective investment after decay transformation)
- `K` = half-saturation point — investment level at which response = 50% of maximum
- `α` = Hill coefficient — controls curve steepness (higher α = sharper S-curve)

**Why Hill over logarithmic:**
1. Passes through zero (no spend → no lift). Logarithmic cannot produce zero.
2. Has an explicit saturation ceiling — allows quantitative identification of diminishing returns
3. K is interpretable: "at K pesos of weekly investment, we get half of maximum possible lift"
4. α and K are stored in MODEL_METADATA per brand × channel, enabling comparison across portfolio

---

## Full MMM Regression Equation

```
sell_out_units_t = β₀
    + β₁ × hill_transform(adstock_t(investment), K, α)
    + β₂ × promotion_active_t
    + β₃ × price_index_vs_category_t
    + β₄ × nielsen_weighted_distribution_t
    + β₅ × is_holiday_t
    + β₆ × sell_out_lag_4w_t
    + ε_t
```

---

## Model Outputs (stored in MODEL_METADATA and MART)

| Output | Description |
|--------|-------------|
| `adstock_decay_rate` | Estimated decay rate per brand × channel |
| `hill_K` | Half-saturation point (pesos/dollars of weekly investment) |
| `hill_alpha` | Curve steepness coefficient |
| `incremental_units_per_peso` | Marginal return at current spend level |
| `saturation_reached_flag` | TRUE if current spend within 10% of K |
| `recommended_spend_range_low` | Lower bound of efficient investment zone |
| `recommended_spend_range_high` | Upper bound of efficient investment zone |

---

## Edge Cases

| Scenario | Risk | Handling |
|----------|------|---------|
| Investment never varied significantly | Hill curve cannot be fit | Flag `saturation_not_observable = TRUE`; present linear estimate only; do not force Hill fit |
| Investment spend winsorized to 99th pct | Extreme spike removed | Winsorize before adstock transformation; document in output |
| Below detection threshold | Spend too small to produce measurable lift | Flag "below detection threshold"; return NULL for incremental estimate |
| Attribution gap (unmeasured media) | ROI overstated | Note "measured investment only" in every dashboard tooltip and model card |

---

## Validation

- **Metric:** Out-of-sample R² on holdout period (last 3 months of history)
- **Directional accuracy:** Months where investment increased — did sell-out lift in same direction?
- **Commercial review:** Present diminishing returns curves to Trade Marketing before dashboard deployment
- **Evaluation frequency:** Quarterly (not weekly — this is not a weekly prediction model)

---

## Approval Log

| Action | Person | Date | Notes |
|--------|--------|------|-------|
| Target variable confirmed | [TBD] | [TBD] | Phase 5 blocker |
| Counterfactual method confirmed | [TBD] | [TBD] | Phase 5 blocker |
| Saturation curve type confirmed | [TBD] | [TBD] | Phase 5 blocker |
| Baseline R² and directional accuracy reviewed | [TBD] | [TBD] | |
| Dashboard curves approved by Trade Marketing | [TBD] | [TBD] | |
