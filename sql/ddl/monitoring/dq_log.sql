-- Data Quality Log Table
-- Records the result of every automated DQ check at each pipeline layer transition.
-- Never delete rows from this table — it is the audit trail for data quality.

CREATE TABLE IF NOT EXISTS MGI_PROD.MONITORING.DQ_LOG (
    dq_log_id         NUMBER AUTOINCREMENT PRIMARY KEY,
    check_timestamp   TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    pipeline_layer    VARCHAR(10) NOT NULL,         -- BRONZE, SILVER, GOLD
    source_table      VARCHAR(100) NOT NULL,
    check_name        VARCHAR(200) NOT NULL,
    check_type        VARCHAR(50) NOT NULL,          -- NULL_RATE, ROW_COUNT, VALUE_RANGE, REFERENTIAL
    check_result      VARCHAR(10) NOT NULL,          -- PASS, WARNING, ERROR
    actual_value      FLOAT,
    threshold_value   FLOAT,
    row_count         NUMBER,
    batch_id          VARCHAR(100),
    error_message     VARCHAR(2000),
    created_at        TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);
