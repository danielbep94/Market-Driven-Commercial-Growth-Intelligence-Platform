-- MART_MARKET_GROWTH_INTELLIGENCE_MONTHLY
-- The single source of truth for the Power BI dashboard and ML feature extraction.
-- Grain: Brand × Channel × Month
-- Pre-aggregated with all KPIs computed.

CREATE TABLE IF NOT EXISTS MGI_PROD.MART.MART_MARKET_GROWTH_INTELLIGENCE_MONTHLY (
    mart_key                        NUMBER AUTOINCREMENT PRIMARY KEY,
    -- Dimensions
    brand_key                       NUMBER NOT NULL,
    brand_name                      VARCHAR(200) NOT NULL,
    channel_key                     NUMBER NOT NULL,
    channel_name                    VARCHAR(200) NOT NULL,
    channel_type                    VARCHAR(50) NOT NULL,
    region_key                      NUMBER NOT NULL,
    month_key                       NUMBER NOT NULL,          -- YYYYMM
    year_number                     NUMBER(4) NOT NULL,
    month_number                    NUMBER(2) NOT NULL,
    fiscal_period                   VARCHAR(20),
    -- Volume KPIs
    sell_in_units                   DECIMAL(18,4),
    sell_out_units                  DECIMAL(18,4),
    waste_units                     DECIMAL(18,4),
    sell_out_revenue                DECIMAL(18,2),
    waste_cost                      DECIMAL(18,2),
    investment_spend                DECIMAL(18,2),
    -- Ratio KPIs
    sell_out_sell_in_ratio          DECIMAL(10,6),            -- NULL if sell_in = 0
    waste_rate                      DECIMAL(10,6),            -- NULL if sell_in = 0
    waste_adjusted_roi              DECIMAL(10,6),            -- NULL if investment = 0
    -- Forecast KPIs
    forecast_units                  DECIMAL(18,4),
    forecast_bias                   DECIMAL(10,6),
    forecast_accuracy_wape          DECIMAL(10,6),
    -- Market KPIs (from Nielsen — may lag 4–8 weeks)
    value_share                     DECIMAL(10,6),
    volume_share                    DECIMAL(10,6),
    share_change_pp                 DECIMAL(10,6),            -- vs. prior period
    market_category_growth_pct      DECIMAL(10,6),            -- from Nielsen
    market_adjusted_growth          DECIMAL(10,6),            -- brand growth - category growth
    numeric_distribution            DECIMAL(10,6),
    weighted_distribution           DECIMAL(10,6),
    price_index_vs_category         DECIMAL(10,6),
    -- Composite scores
    growth_quality_score            DECIMAL(10,4),            -- 0–100
    gqs_confidence_level            VARCHAR(10),              -- HIGH, MEDIUM, LOW, NULL
    gqs_version                     VARCHAR(10),              -- e.g., "v1"
    waste_risk_score                DECIMAL(10,6),            -- 0–1 (ML output)
    waste_risk_category             VARCHAR(10),              -- LOW, MEDIUM, HIGH
    competitive_risk_score          DECIMAL(10,4),            -- 0–100
    -- Recommendation (from Recommendation Engine)
    recommended_action              VARCHAR(20),              -- INCREASE, MAINTAIN, OPTIMIZE, REDUCE
    recommendation_rationale        VARCHAR(2000),
    recommendation_version          VARCHAR(10),
    -- Data quality / confidence
    sell_out_coverage_pct           DECIMAL(10,4),            -- % of expected stores/customers with data
    nielsen_lag_weeks               NUMBER(2),                -- Lag vs. internal data
    data_confidence_overall         VARCHAR(10),              -- HIGH, MEDIUM, LOW
    -- Audit
    computed_at                     TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    source_pipeline_run_id          VARCHAR(100)
);
COMMENT ON TABLE MGI_PROD.MART.MART_MARKET_GROWTH_INTELLIGENCE_MONTHLY IS
    'Single source of truth for Power BI dashboard and ML features. Grain: Brand x Channel x Month. All KPIs pre-computed.';
