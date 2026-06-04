-- MODEL_PERFORMANCE_LOG
-- Records weekly prediction vs. actual for every ML model.
-- This table is what makes retraining triggers detectable — without it,
-- the "WAPE degrades > 5pp" trigger cannot be detected automatically.
-- Written by: notebooks/phase5_ml/model_monitoring/weekly_performance_check.py
-- Run: Every Monday after Gold layer is written to Snowflake.
-- NEVER delete rows — this is the audit trail for model performance.

CREATE TABLE IF NOT EXISTS MGI_PROD.MONITORING.MODEL_PERFORMANCE_LOG (
    log_id                  NUMBER AUTOINCREMENT PRIMARY KEY,
    log_week                DATE NOT NULL,           -- ISO week start date of evaluation period
    model_name              VARCHAR(100) NOT NULL,   -- e.g., demand_forecast_lgbm_modern_trade
    model_version           VARCHAR(50) NOT NULL,    -- MLflow registered version
    channel                 VARCHAR(50),             -- Channel if model is channel-specific
    brand_key               NUMBER,                  -- NULL = portfolio-level metric
    -- Primary performance metrics
    wape_actual             FLOAT NOT NULL,          -- WAPE computed this week
    wape_baseline           FLOAT NOT NULL,          -- WAPE at time of training (from MODEL_METADATA)
    wape_delta_pp           FLOAT GENERATED ALWAYS AS (wape_actual - wape_baseline),  -- Positive = degraded
    bias_actual             FLOAT,
    -- Classification metrics (Waste Risk model only)
    recall_actual           FLOAT,
    precision_actual        FLOAT,
    recall_baseline         FLOAT,
    recall_delta            FLOAT GENERATED ALWAYS AS (recall_actual - recall_baseline),  -- Negative = degraded
    -- Trigger detection
    wape_trigger_fired      BOOLEAN NOT NULL DEFAULT FALSE,    -- TRUE if wape_delta_pp > threshold
    recall_trigger_fired    BOOLEAN NOT NULL DEFAULT FALSE,    -- TRUE if recall_delta < -threshold
    retrain_triggered       BOOLEAN NOT NULL DEFAULT FALSE,    -- TRUE if retraining job was dispatched
    alert_sent              BOOLEAN NOT NULL DEFAULT FALSE,    -- TRUE if alert notification was sent
    alert_recipient         VARCHAR(500),
    -- Counts for context
    n_brands_evaluated      NUMBER,
    n_skus_evaluated        NUMBER,
    -- Audit
    computed_at             TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    pipeline_run_id         VARCHAR(100)
);

COMMENT ON TABLE MGI_PROD.MONITORING.MODEL_PERFORMANCE_LOG IS
    'Weekly prediction vs. actual per ML model. Enables automated retraining trigger detection.
     Written by weekly_performance_check.py after Gold layer is written. Never delete rows.';
