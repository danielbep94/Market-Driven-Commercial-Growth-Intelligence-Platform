-- FACT_SELL_IN
-- Grain: SKU × Customer × Week
-- Source: Sell-in / shipment data from ERP

CREATE TABLE IF NOT EXISTS MGI_PROD.GOLD.FACT_SELL_IN (
    sell_in_key         NUMBER AUTOINCREMENT PRIMARY KEY,
    -- Dimension keys
    product_key         NUMBER NOT NULL,    -- FK to DIM_PRODUCT
    customer_key        NUMBER NOT NULL,    -- FK to DIM_CUSTOMER
    channel_key         NUMBER NOT NULL,    -- FK to DIM_CHANNEL
    brand_key           NUMBER NOT NULL,    -- FK to DIM_BRAND (denormalized for query performance)
    region_key          NUMBER NOT NULL,    -- FK to DIM_REGION
    week_key            NUMBER NOT NULL,    -- FK to DIM_DATE (week start date key)
    -- Measures
    units_shipped       DECIMAL(18,4) NOT NULL,
    net_revenue         DECIMAL(18,2),
    list_price          DECIMAL(18,4),
    net_price           DECIMAL(18,4),
    -- Quality flags
    is_outlier          BOOLEAN NOT NULL DEFAULT FALSE,
    confidence_level    VARCHAR(10),        -- HIGH, MEDIUM, LOW
    -- Audit
    batch_id            VARCHAR(100) NOT NULL,
    source_table        VARCHAR(100) NOT NULL,
    ingested_at         TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);
COMMENT ON TABLE MGI_PROD.GOLD.FACT_SELL_IN IS 'Sell-in (shipments). Grain: SKU x Customer x Week. Source: ERP.';
