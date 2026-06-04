-- Homologation Exceptions Table
-- Records all values that could not be mapped by the homologation dictionary.
-- Data Steward must review and resolve these before records can flow to Gold.

CREATE TABLE IF NOT EXISTS MGI_PROD.MONITORING.HOMOLOGATION_EXCEPTIONS (
    exception_id          NUMBER AUTOINCREMENT PRIMARY KEY,
    detected_at           TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    source_table          VARCHAR(100) NOT NULL,
    field_name            VARCHAR(100) NOT NULL,   -- e.g., brand_name, channel_name
    raw_value             VARCHAR(500) NOT NULL,
    source_system         VARCHAR(100),
    batch_id              VARCHAR(100),
    row_count_affected    NUMBER NOT NULL,
    resolution_status     VARCHAR(20) DEFAULT 'PENDING',  -- PENDING, RESOLVED, EXCLUDED
    resolved_by           VARCHAR(100),
    resolved_at           TIMESTAMP_NTZ,
    clean_value_assigned  VARCHAR(500),
    notes                 VARCHAR(2000)
);
