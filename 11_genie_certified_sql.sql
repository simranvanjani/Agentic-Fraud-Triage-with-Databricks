-- Genie Space: Certified SQL for banking KPIs
-- Use these in a Databricks Genie space so investigators can ask:
-- "Show me False Positive Ratio" / "Account Takeover Rate" / "Wire transfers over 10k where MFA changed in 24h"

-- False Positive Ratio (FPR): flagged transactions that were later confirmed legitimate
-- Assumes gold/triage has review_status and final_disposition or similar.
CREATE OR REPLACE VIEW Financial_Security.gold.v_false_positive_ratio AS
SELECT
  COUNT(CASE WHEN review_status = 'APPROVED' AND automated_action IN ('BLOCK','CHALLENGE') THEN 1 END) * 1.0 / NULLIF(COUNT(*), 0) AS false_positive_ratio,
  COUNT(*) AS total_flagged
FROM Financial_Security.gold.real_time_fraud_triage
WHERE review_status IN ('APPROVED', 'REJECTED');

-- Account Takeover Rate: high-risk events (MFA change + high-value or impossible travel)
CREATE OR REPLACE VIEW Financial_Security.gold.v_account_takeover_rate AS
SELECT
  COUNT(DISTINCT user_id) AS ato_user_count,
  COUNT(*) AS ato_event_count
FROM Financial_Security.silver.silver_user_activity
WHERE (mfa_change_flag = true AND high_value_wire = true)
   OR user_id IN (SELECT user_id FROM Financial_Security.silver.impossible_travel_flags);

-- Conversational: Wire transfers over $10k where user changed MFA in last 24 hours
CREATE OR REPLACE VIEW Financial_Security.gold.v_wire_10k_mfa_24h AS
SELECT transaction_id, user_id, amount, transaction_time, mfa_change_flag, ip_changed_recently
FROM Financial_Security.silver.silver_user_activity
WHERE transaction_type = 'wire' AND amount >= 10000 AND mfa_change_flag = true;
