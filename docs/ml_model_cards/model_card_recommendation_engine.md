# Model Card — Recommendation Engine

**Version:** 1.0 DRAFT  
**Type:** Rule-based (NOT machine learning)  
**Status:** PENDING commercial leadership review  
**Rules configuration:** `models/recommendation_engine/rules_config.yaml`

## Why Rule-Based?
The Recommendation Engine uses explicit, interpretable rules — not a black-box ML model.
This is a deliberate design choice. Commercial adoption depends on trust. A category manager
must be able to understand and agree with every recommendation before acting on it.
ML can be added in a future version after rules are validated and trusted.

## Conflict Resolution Priority

1. **Waste Safety** — If Waste Risk Score > 70: always flag OPTIMIZE + waste review, regardless of growth
2. **ROI Efficiency** — If Waste-Adjusted ROI < 1.0: REDUCE (unless share is growing)
3. **Growth Quality** — If GQS > 70: MAINTAIN or INCREASE
4. **Default** — Insufficient signal → WATCH

## Output Actions

| Action | Meaning |
|--------|---------|
| INCREASE | Increase investment in this brand × channel |
| MAINTAIN | Keep current strategy; no change recommended |
| OPTIMIZE | Same spend, different allocation (channel, promotion type) |
| REDUCE | Reduce investment; review for potential exit |
| WATCH | Insufficient signal; monitor next period |

## Explainability Requirements
Every recommendation in the dashboard must include:
- The action taken
- The rationale text (plain language, no jargon)
- The top 2 signals that triggered the rule
- The estimated impact range (where available)

## Approval Required
Rules must be co-designed with commercial leadership. See `docs/governance/` for sign-off process.
