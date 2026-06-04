# Model Card — Competitive Risk Engine

**Version:** v1.0 (rule-based weighted composite)
**Status:** DESIGN PHASE
**Type:** Rule-based computation (not a trainable ML model in v1)
**Owner:** Data Scientist

> [!IMPORTANT]
> v1 does NOT use clustering. This decision must be communicated to the commercial
> team in Phase 0. The clustering approach from the original plan is deferred to v2.
> See §v2 Plan below.

---

## Business Output
Score 0–100 indicating current competitive vulnerability of each brand.
Based on five observable Nielsen signals. Higher score = higher risk.
Alerts when score > 70 for 2 consecutive periods.

---

## v1 Design — Weighted Composite Score

### Why not clustering for v1?

Clustering was specified in v2.1 but left critical design questions undefined:
- What features define the clustering? (not specified)
- How many clusters? (not specified)
- How does cluster membership become a score? (not clear — reduces to two features anyway)

A simple weighted composite answers "why" for every score (transparent + trustworthy), is faster to build, and is immediately validatable against historical share movements. Clustering adds value only when you have enough brand-period history (≥24 months, ≥10 brands/category) to identify non-obvious combinations of signals. That data won't exist at launch.

### Feature Vector (all from FACT_NIELSEN_MARKET)

| Feature | Weight | Direction | Normalization |
|---------|--------|-----------|--------------|
| `share_change_pp` (13-week trend) | 30% | More negative = higher risk | Scaled to category benchmark range |
| Rate of change of `price_index_vs_category` | 25% | Rising gap = higher risk | Scaled to ±20% range |
| `numeric_distribution_change_13w` | 20% | Declining = higher risk | Scaled to observed range |
| `weighted_distribution_change_13w` | 15% | Declining = higher risk | Scaled to observed range |
| `share_yoy_change_pp` | 10% | Declining = higher risk | Scaled to ±5pp range |

All normalized to 0–100 before weighting. Final score: 0–100.

### Formula

```
competitive_risk_score_v1 = 0.30 × share_loss_score
                           + 0.25 × price_gap_score
                           + 0.20 × numeric_dist_score
                           + 0.15 × weighted_dist_score
                           + 0.10 × share_yoy_score
```

### Partial Score Handling (Nielsen data gaps)

- 1 component NULL → redistribute weight proportionally; set `confidence = PARTIAL`
- 2 components NULL → redistribute; set `confidence = LOW`
- > 2 components NULL → return NULL score
- Single brand in category → return NULL; flag "no competitive context"

### SQL Implementation

Stored as a versioned Snowflake function: `fn_competitive_risk_score_v1.sql`
Computed weekly from Gold tables. No training step. Pure computation.

---

## Validation (Retrospective)

**Method:** For each brand flagged HIGH risk (score > 70) in week N, check whether actual share change in weeks N+1 to N+8 was ≤ −0.5pp.

**Metrics:**
- True Positive Rate (recall): proportion of actual share loss events that were flagged in advance
- False Positive Rate: proportion of HIGH flags where no share loss occurred

Document results in `docs/phase_outputs/` and this model card before production deployment.

---

## Alert Logic

Score > 70 for **2 consecutive periods** → automatic flag in dashboard.
Single-period spike does not alert (avoids noise from Nielsen data anomalies).
Alert field: `competitive_risk_alert_flag` in MART table.

---

## v2 Plan — Clustering (Deferred)

**When to consider v2:** When ≥ 24 months of history is available for ≥ 10 brands per category.

**Feature vector for clustering (planned):**
- Share level
- 13-week share change
- Price index vs. category
- Numeric distribution
- Weighted distribution
- Category growth rate

**Clustering approach (planned):** K-means or hierarchical clustering. Number of clusters determined by silhouette score on held-out data. Risk score derived from cluster centroid distance (not just two features).

**Validation (planned):** Compare clustering-derived scores against v1 composite scores on same historical data. Adopt clustering only if it demonstrates meaningfully higher true positive rate.

---

## Approval Log

| Action | Person | Date |
|--------|--------|------|
| v1 scope (rule-based composite) communicated to commercial team | [TBD] | [TBD] |
| Feature weights reviewed by Market Intelligence | [TBD] | [TBD] |
| Retrospective validation results reviewed | [TBD] | [TBD] |
| v1 production deployment approved | [TBD] | [TBD] |
