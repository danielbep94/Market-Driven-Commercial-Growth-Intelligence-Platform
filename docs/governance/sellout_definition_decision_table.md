# Sell-out Definition Decision Table

**Status:** ⛔ UNSIGNED — PHASE 0 BLOCKER  
**Required before:** Phase 3 (Bronze/Silver processing) can begin  
**Owner:** Commercial Leadership  
**Sign-off required:** [COMMERCIAL LEAD NAME] | [DATE]

---

> **Why this is the most important document in the project:**
>
> The Sell-out / Sell-in Ratio, Waste Rate, Forecast Accuracy, Growth Quality Score,
> Demand Forecast model, and Waste Risk model ALL use sell-out as their foundation.
>
> If sell-out is defined differently across channels — POS scanner in modern trade,
> retailer-reported in some accounts, and sell-in minus inventory delta in traditional
> trade — then every KPI is computing a different thing for different brands, and
> **no cross-brand comparison is valid**.
>
> This is a business decision. It cannot be made by data engineering.
> It must be signed off here before Phase 3 begins.

---

## Decision Table — One Row Per Channel

| Channel type | Agreed measurement method | Source system | Expected coverage | Known structural gaps | Confidence level | Signed off by | Date |
|--------------|--------------------------|--------------|------------------|--------------------|-----------------|--------------|------|
| Modern trade | [POS scanner / retailer-reported / estimated?] | [TBD] | [TBD]% of stores | [TBD] | [HIGH/MEDIUM/LOW] | [TBD] | [TBD] |
| Traditional trade | [POS scanner / retailer-reported / sell-in minus inventory delta?] | [TBD] | [Likely <70%] | Most tiendas do not report POS data | [LOW expected] | [TBD] | [TBD] |
| E-commerce | [Platform-reported orders / API extract?] | [TBD] | [TBD]% of orders | [TBD] | [TBD] | [TBD] | [TBD] |
| Foodservice | [Estimated / distributor-reported?] | [TBD] | [TBD] | [TBD] | [TBD] | [TBD] | [TBD] |

---

## Reference: Measurement Method Comparison

| Method | What it measures | Bias risk | Suitable for cross-brand comparison? |
|--------|-----------------|-----------|--------------------------------------|
| POS scanner | True consumer purchase at shelf (checkout scan) | Low | ✅ Yes — gold standard |
| Retailer-reported sell-through | Retailer's own inventory depletion record | Medium — depends on retailer process accuracy | ✅ Yes, with MEDIUM confidence flag |
| Sell-in minus inventory delta | Implied sell-out — not directly observed | High — inventory accuracy errors compound | ⚠ Only if inventory data is reliable; flag as ESTIMATED |
| Distributor-reported | Distributor's sell-out to end customer | Medium — coverage incomplete | ⚠ Flag as partial coverage |

---

## Implementation Consequence

The agreed method per channel determines the value of the
`sell_out_measurement_method` field in `FACT_SELL_OUT`:

| Method agreed | Field value |
|--------------|-------------|
| POS scanner | `POS_SCANNER` |
| Retailer-reported | `RETAILER_REPORTED` |
| Sell-in minus inventory delta | `ESTIMATED` |
| Distributor-reported | `DISTRIBUTOR_REPORTED` |

All KPI queries and dashboard tooltips display this field so stakeholders always know
what type of sell-out they are looking at.

KPIs computed from `ESTIMATED` sell-out are **automatically assigned `confidence_level = LOW`**
regardless of coverage percentage.

---

## Sign-off

By signing below, the commercial leadership team confirms:
1. The measurement method per channel is agreed and documented above
2. The team understands that cross-channel KPI comparisons will reflect different data sources
3. The confidence level assigned per channel is accepted and will be shown to stakeholders in the dashboard

| Name | Role | Signature | Date |
|------|------|---------|------|
| | | | |
| | | | |
