# Power BI Page 1 — Executive Overview

**Audience:** CEO, CCO, VP Commercial  
**Decision enabled:** Portfolio health at a glance; where to focus this week  
**Design rule:** Maximum 7 data points in a single view. Complexity belongs on deeper pages.

## KPI Cards (top row — 4 cards)

| Card | Metric | Format | Trend indicator |
|------|--------|--------|----------------|
| 1 | Avg Growth Quality Score (portfolio) | `72 / 100` | ↑ +4 pts vs. prior period |
| 2 | Brands at waste risk | `3 / 12` | ↑ +1 vs. last month |
| 3 | Portfolio forecast accuracy (WAPE) | `81%` | ↓ –3 pts vs. target |
| 4 | Waste-Adjusted ROI | `2.1x` | ↑ +0.2x vs. prior period |

**Color coding:** Green (on target), Amber (watch), Red (below threshold). Thresholds defined in `configs/kpi_weights.yaml`.

## Brand Growth Scorecard (main table)

Columns: Brand | Sell-out trend | Share Δ | Waste rate | Forecast accuracy | GQ Score | Action

**Conditional formatting:**
- Sell-out trend: Green (>0%), Amber (–5% to 0%), Red (<–5%)
- Share Δ: Green (>0pp), Red (<0pp)
- Waste rate: Green (<threshold), Amber (threshold to 2x), Red (>2x threshold)
- GQ Score: Color bar 0–100 (Red–Yellow–Green)
- Action: Color-coded badge (Increase=Green, Maintain=Blue, Optimize=Amber, Reduce=Red)

**Confidence flag:** If `data_confidence_overall` = LOW or MEDIUM, show ⚠️ icon next to brand name.

## Investment Efficiency Bar Chart (right side)

Brand × Waste-Adjusted ROI. Horizontal bars. Sorted descending. Reference line at ROI = 1.0.

## ML Recommendation Matrix (bottom right — 4 quadrants)

| | Increase | Maintain |
|-|---------|---------|
| | Optimize | Reduce |

Each quadrant lists brand × channel combinations. Clicking opens Page 7 (Recommendation Matrix) filtered to that brand.

## Info Panel (accessible via ? icon)

All KPI definitions accessible from within the page. Tooltip on every metric label.

## RLS
This page shows all brands. No RLS filter for Executive role.
