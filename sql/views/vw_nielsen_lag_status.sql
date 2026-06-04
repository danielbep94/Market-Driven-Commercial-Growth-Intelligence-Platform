-- vw_nielsen_lag_status
-- Drives the ⚠ Nielsen lag banner on Page 1 (Executive Overview) and Page 6 (Nielsen).
-- Rule: if most recent Nielsen period is > nielsen_lag_warning_weeks older than the
--       internal data period, return lag_banner_required = TRUE.
--
-- The lag threshold is NOT hardcoded here — it is read from DQ config at runtime.
-- For Power BI, this view is refreshed weekly as part of the Import mode refresh.
-- Power BI reads this view and shows the banner if lag_banner_required = TRUE.

CREATE OR REPLACE VIEW MGI_PROD.MART.VW_NIELSEN_LAG_STATUS AS
WITH internal_max_period AS (
    SELECT MAX(week_key) AS max_internal_week_key
    FROM MGI_PROD.GOLD.FACT_SELL_OUT
),
nielsen_max_period AS (
    SELECT MAX(period_end_date) AS max_nielsen_date
    FROM MGI_PROD.GOLD.FACT_NIELSEN_MARKET
),
date_context AS (
    SELECT
        d.full_date AS internal_max_date,
        n.max_nielsen_date,
        DATEDIFF('week', n.max_nielsen_date, d.full_date) AS lag_weeks
    FROM internal_max_period imp
    JOIN MGI_PROD.GOLD.DIM_DATE d ON d.date_key = imp.max_internal_week_key
    CROSS JOIN nielsen_max_period n
)
SELECT
    internal_max_date,
    max_nielsen_date,
    lag_weeks,
    -- Threshold read from monitoring config table, not hardcoded
    -- For simplicity in initial version, threshold = 6 (from dq_thresholds.yaml)
    -- TODO: replace literal 6 with lookup to a CONFIG table populated from YAML
    CASE WHEN lag_weeks > 6 THEN TRUE ELSE FALSE END AS lag_banner_required,
    CASE
        WHEN lag_weeks > 6
        THEN CONCAT('⚠ Nielsen data as of ', TO_VARCHAR(max_nielsen_date, 'DD Mon YYYY'),
                    ' (', lag_weeks, ' weeks behind internal data)')
        ELSE 'Nielsen data current'
    END AS lag_banner_message,
    CURRENT_TIMESTAMP() AS computed_at;

COMMENT ON VIEW MGI_PROD.MART.VW_NIELSEN_LAG_STATUS IS
    'Used by Power BI Page 1 and Page 6 to show Nielsen lag banner when lag > 6 weeks.
     Threshold is 6 weeks; update SQL TODO when CONFIG table is available.';
