-- fn_waste_rate_v1
-- Waste Rate = waste_units / sell_in_units
-- Edge cases:
--   sell_in_units = 0     → NULL (cannot divide; do not impute zero)
--   waste_units > sell_in → Flag anomaly; return raw value but mark is_anomaly = TRUE
-- Version: 1.0 | Approved by: [TBD] | Date: [TBD]

CREATE OR REPLACE FUNCTION MGI_PROD.GOLD.FN_WASTE_RATE_V1(
    waste_units    FLOAT,
    sell_in_units  FLOAT
)
RETURNS OBJECT
LANGUAGE JAVASCRIPT
AS $$
    if (SELL_IN_UNITS === null || SELL_IN_UNITS === 0) {
        return { waste_rate: null, is_anomaly: false, anomaly_reason: (SELL_IN_UNITS === 0 ? 'SELL_IN_ZERO' : 'SELL_IN_NULL') };
    }
    if (WASTE_UNITS === null) {
        return { waste_rate: null, is_anomaly: false, anomaly_reason: 'WASTE_NULL' };
    }
    var rate = WASTE_UNITS / SELL_IN_UNITS;
    var is_anomaly = (rate > 1.0);
    return {
        waste_rate: rate,
        is_anomaly: is_anomaly,
        anomaly_reason: is_anomaly ? 'WASTE_EXCEEDS_SELL_IN' : null
    };
$$;
COMMENT ON FUNCTION MGI_PROD.GOLD.FN_WASTE_RATE_V1 IS 'Waste Rate v1. Returns object with waste_rate (NULL if sell_in=0), is_anomaly flag, and anomaly_reason. Business approved: [TBD].';
