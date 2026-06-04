-- fn_sell_out_sell_in_ratio_v1
-- Ratio = sell_out_units / sell_in_units
-- Edge cases:
--   sell_in = 0    → NULL
--   ratio > 1      → Valid (retailer sold from prior inventory); allowed; visualize with overflow label
--   ratio > 1.5    → Flag for review
-- Version: 1.0 | Approved by: [TBD] | Date: [TBD]

CREATE OR REPLACE FUNCTION MGI_PROD.GOLD.FN_SELL_OUT_SELL_IN_RATIO_V1(
    sell_out_units FLOAT,
    sell_in_units  FLOAT
)
RETURNS OBJECT
LANGUAGE JAVASCRIPT
AS $$
    if (SELL_IN_UNITS === null || SELL_IN_UNITS === 0) {
        return { ratio: null, is_valid: false, flag: 'SELL_IN_ZERO' };
    }
    if (SELL_OUT_UNITS === null) {
        return { ratio: null, is_valid: false, flag: 'SELL_OUT_NULL' };
    }
    var ratio = SELL_OUT_UNITS / SELL_IN_UNITS;
    var flag = null;
    if (ratio > 1.5) flag = 'RATIO_EXCEEDS_1_5';
    else if (ratio > 1.0) flag = 'RATIO_ABOVE_1_PRIOR_INVENTORY';
    return { ratio: ratio, is_valid: true, flag: flag };
$$;
