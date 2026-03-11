-- Lakebase: Operational Triage Store (real_time_fraud_triage)
-- Sub-second blocking; high-concurrency, low-latency access by core banking engine.
-- Run this in Lakebase (Serverless Postgres) or via JDBC from Databricks.

CREATE TABLE IF NOT EXISTS real_time_fraud_triage (
    transaction_id       VARCHAR(255) PRIMARY KEY,
    user_id              VARCHAR(255) NOT NULL,
    risk_score            DECIMAL(5,2) NOT NULL,
    risk_level            VARCHAR(20) NOT NULL,
    automated_action      VARCHAR(20) NOT NULL,
    action_reason         TEXT,
    action_timestamp      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    review_status         VARCHAR(20) DEFAULT 'PENDING',
    reviewed_by           VARCHAR(255),
    review_timestamp      TIMESTAMP,
    created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_triage_risk_level ON real_time_fraud_triage(risk_level);
CREATE INDEX IF NOT EXISTS idx_triage_action ON real_time_fraud_triage(automated_action);
CREATE INDEX IF NOT EXISTS idx_triage_review ON real_time_fraud_triage(review_status);
CREATE INDEX IF NOT EXISTS idx_triage_created ON real_time_fraud_triage(created_at DESC);

COMMENT ON TABLE real_time_fraud_triage IS 'Operational fraud triage for <200ms Block/Allow decisions';
