-- DIM_BRAND — Brand Dimension
-- SCD Type 1 (overwrite) for display attributes; SCD Type 2 for category assignment.
-- All brand names must come from the homologation dictionary.

CREATE TABLE IF NOT EXISTS MGI_PROD.GOLD.DIM_BRAND (
    brand_key           NUMBER NOT NULL PRIMARY KEY,  -- Surrogate key
    brand_id            VARCHAR(50) NOT NULL,          -- Natural key from source
    brand_name          VARCHAR(200) NOT NULL,         -- Clean name from homologation
    brand_name_display  VARCHAR(200),                  -- Display name (may include accents)
    category_key        NUMBER,                        -- FK to DIM_CATEGORY
    sub_category        VARCHAR(100),
    brand_owner         VARCHAR(200),
    -- SCD Type 2 fields
    effective_date      DATE NOT NULL,
    expiry_date         DATE,
    is_current          BOOLEAN NOT NULL DEFAULT TRUE,
    -- Metadata
    created_at          TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);
