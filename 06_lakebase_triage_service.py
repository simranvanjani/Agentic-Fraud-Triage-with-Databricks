# Databricks notebook source
# MAGIC %md
# MAGIC # Triage Service: Upsert Risk Scores to Lakebase
# MAGIC Sub-second path: read risk from agent/gold, upsert into Lakebase real_time_fraud_triage for blocking.

# COMMAND ----------

import os
import time

# Use asyncpg or psycopg2; for <200ms use connection pool and single upsert.
try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    %pip install psycopg2-binary
    import psycopg2

# COMMAND ----------

# Lakebase connection: set via env or Databricks secrets
LAKEBASE_HOST = os.environ.get("PGHOST", "your-lakebase-host.database.azuredatabricks.net")
LAKEBASE_PORT = int(os.environ.get("PGPORT", "5432"))
LAKEBASE_DB = os.environ.get("PGDATABASE", "databricks_postgres")
LAKEBASE_USER = os.environ.get("PGUSER", "user@example.com")
LAKEBASE_PASSWORD = os.environ.get("PGPASSWORD") or dbutils.secrets.get(scope="lakebase", key="password")

TABLE = "real_time_fraud_triage"

def get_conn():
    return psycopg2.connect(
        host=LAKEBASE_HOST,
        port=LAKEBASE_PORT,
        dbname=LAKEBASE_DB,
        user=LAKEBASE_USER,
        password=LAKEBASE_PASSWORD,
        sslmode="require",
        connect_timeout=5,
    )

# COMMAND ----------

def upsert_triage(transaction_id: str, user_id: str, risk_score: float, risk_level: str, automated_action: str, action_reason: str):
    """Single-row upsert for <200ms path. Call from streaming or real-time job."""
    sql = """
    INSERT INTO real_time_fraud_triage (transaction_id, user_id, risk_score, risk_level, automated_action, action_reason)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (transaction_id) DO UPDATE SET
        risk_score = EXCLUDED.risk_score,
        risk_level = EXCLUDED.risk_level,
        automated_action = EXCLUDED.automated_action,
        action_reason = EXCLUDED.action_reason,
        action_timestamp = CURRENT_TIMESTAMP
    """
    start = time.perf_counter()
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (transaction_id, user_id, risk_score, risk_level, automated_action, action_reason))
        conn.commit()
    finally:
        conn.close()
    elapsed_ms = (time.perf_counter() - start) * 1000
    return {"ok": True, "elapsed_ms": round(elapsed_ms, 2)}

# COMMAND ----------

# Example: upsert one record (replace with your risk scoring output)
# upsert_triage("TXN-00000001", "USER-00001", 85.0, "HIGH", "CHALLENGE", "High-value wire within 1h of IP change.")
