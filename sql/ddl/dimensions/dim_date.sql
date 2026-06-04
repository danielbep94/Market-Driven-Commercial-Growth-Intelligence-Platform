-- DIM_DATE — Date Dimension
-- Must cover full historical period plus 52-week forecast horizon.
-- Fiscal calendar attributes must be verified with Finance before implementation.

CREATE TABLE IF NOT EXISTS MGI_PROD.GOLD.DIM_DATE (
    date_key            NUMBER NOT NULL PRIMARY KEY,  -- Format: YYYYMMDD
    full_date           DATE NOT NULL,
    day_of_week         NUMBER(1) NOT NULL,           -- 1=Monday, 7=Sunday
    day_name            VARCHAR(10) NOT NULL,
    day_of_month        NUMBER(2) NOT NULL,
    day_of_year         NUMBER(3) NOT NULL,
    week_number         NUMBER(2) NOT NULL,           -- ISO week
    week_start_date     DATE NOT NULL,
    week_end_date       DATE NOT NULL,
    month_number        NUMBER(2) NOT NULL,
    month_name          VARCHAR(10) NOT NULL,
    quarter_number      NUMBER(1) NOT NULL,
    year_number         NUMBER(4) NOT NULL,
    -- Fiscal calendar (to be verified with Finance)
    fiscal_week         NUMBER(2),
    fiscal_month        NUMBER(2),
    fiscal_quarter      NUMBER(1),
    fiscal_year         NUMBER(4),
    fiscal_period_name  VARCHAR(20),
    -- Business flags
    is_holiday          BOOLEAN NOT NULL DEFAULT FALSE,
    holiday_name        VARCHAR(100),
    holiday_country     VARCHAR(50),
    is_promotion_period BOOLEAN NOT NULL DEFAULT FALSE,
    is_last_day_of_month BOOLEAN NOT NULL DEFAULT FALSE,
    is_last_day_of_quarter BOOLEAN NOT NULL DEFAULT FALSE,
    -- Metadata
    created_at          TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);
COMMENT ON TABLE MGI_PROD.GOLD.DIM_DATE IS 'Date dimension. Must span full history + 52-week forecast horizon. Fiscal attributes require Finance sign-off.';
