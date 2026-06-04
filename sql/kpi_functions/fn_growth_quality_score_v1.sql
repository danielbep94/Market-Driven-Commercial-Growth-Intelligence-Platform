-- fn_growth_quality_score_v1
-- Composite index: 0–100
-- Weights: sell_out_growth 25%, share_change 25%, waste_rate 20%, forecast_accuracy 15%, roi 15%
-- IMPORTANT: Weights must be approved by commercial leadership before use in production.
-- See: docs/kpi_definitions/growth_quality_score_methodology.md
-- Version: 1.0 | Approved by: [PENDING] | Date: [PENDING]

CREATE OR REPLACE FUNCTION MGI_PROD.GOLD.FN_GROWTH_QUALITY_SCORE_V1(
    sell_out_growth_score     FLOAT,   -- 0–100
    share_change_score        FLOAT,   -- 0–100
    waste_rate_score          FLOAT,   -- 0–100 (inverted: 100 = zero waste)
    forecast_accuracy_score   FLOAT,   -- 0–100 (100 = perfect accuracy)
    roi_score                 FLOAT    -- 0–100
)
RETURNS OBJECT
LANGUAGE JAVASCRIPT
AS $$
    // Defined weights (v1)
    var weights = {
        sell_out_growth:   0.25,
        share_change:      0.25,
        waste_rate:        0.20,
        forecast_accuracy: 0.15,
        roi:               0.15
    };

    var components = {
        sell_out_growth:   SELL_OUT_GROWTH_SCORE,
        share_change:      SHARE_CHANGE_SCORE,
        waste_rate:        WASTE_RATE_SCORE,
        forecast_accuracy: FORECAST_ACCURACY_SCORE,
        roi:               ROI_SCORE
    };

    // Count null components
    var null_weight_total = 0;
    var available_components = {};
    for (var key in components) {
        if (components[key] === null) {
            null_weight_total += weights[key];
        } else {
            available_components[key] = components[key];
        }
    }

    // Determine confidence level
    var null_count = Object.keys(components).length - Object.keys(available_components).length;
    var confidence = null_count === 0 ? 'HIGH' : (null_count === 1 ? 'MEDIUM' : 'LOW');

    // Return NULL if more than 2 components are missing
    if (null_count > 2) {
        return { score: null, confidence: 'NULL', null_component_count: null_count };
    }

    // Redistribute null weights proportionally
    var adjusted_weights = {};
    var available_weight_total = 1.0 - null_weight_total;
    for (var key in available_components) {
        adjusted_weights[key] = weights[key] / available_weight_total;
    }

    // Compute score
    var score = 0;
    for (var key in available_components) {
        score += available_components[key] * adjusted_weights[key];
    }

    return {
        score: Math.round(Math.min(100, Math.max(0, score)) * 10) / 10,
        confidence: confidence,
        null_component_count: null_count,
        weight_version: 'v1'
    };
$$;
COMMENT ON FUNCTION MGI_PROD.GOLD.FN_GROWTH_QUALITY_SCORE_V1 IS
    'Growth Quality Score v1. Handles partial scores by redistributing weights. Confidence: HIGH/MEDIUM/LOW/NULL. WEIGHTS PENDING COMMERCIAL SIGN-OFF.';
