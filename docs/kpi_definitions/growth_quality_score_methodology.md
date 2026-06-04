# Growth Quality Score (GQS) — Methodology Note

**Version:** 1.0 DRAFT  
**Sign-off required:** Commercial Leadership  
**Sign-off date:** [TBD]

## Purpose
The GQS is a single number (0–100) that summarizes the commercial health of a brand's growth. It is designed to prevent leadership from treating high-volume growth as healthy when it is driven by channel stuffing, waste accumulation, poor forecast accuracy, or unsustainable investment.

## Component Scoring Logic (proposed)

| Component | Raw KPI | Scoring rule | Weight |
|-----------|---------|-------------|--------|
| Sell-out growth | YoY sell-out growth % | 0–100 scaled to category benchmark | 25% |
| Share change | Nielsen share change pp | 0–100 scaled to ±3pp range | 25% |
| Waste efficiency | Waste rate | 100 − (waste_rate / threshold × 100), min 0 | 20% |
| Forecast quality | 1 − WAPE | Scaled 0–100 | 15% |
| ROI efficiency | Waste-Adjusted ROI | 0–100 scaled to 0–3x range | 15% |

## Partial Score Handling
When one or more components are NULL:
1. Redistribute the NULL component's weight equally to available components
2. Add a `gqs_confidence` flag: HIGH (all components present), MEDIUM (1 component missing), LOW (2+ components missing)
3. Never impute a NULL component with zero — zero sell-out growth and zero waste are very different things

## Annual Recalibration
Weights should be reviewed annually with commercial leadership. Any change requires:
1. A new version of this document
2. A new SQL function version (fn_growth_quality_score_v2.sql)
3. Historical recalculation to compare old vs. new scoring
4. Dashboard annotation marking the methodology change date
