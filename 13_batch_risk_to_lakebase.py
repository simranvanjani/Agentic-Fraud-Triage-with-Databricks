# Databricks notebook source
# MAGIC %md
# MAGIC # Batch: Silver → Reasoning Agent → Lakebase + Gold Delta
# MAGIC Runs reasoning agent, upserts to Lakebase (operational) and to UC gold table (for Genie FPR view).

# COMMAND ----------

# MAGIC %run ./09_reasoning_agent
# MAGIC %run ./06_lakebase_triage_service

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp, lit

CATALOG = "Financial_Security"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"
SILVER_TABLE = f"{CATALOG}.{SCHEMA_SILVER}.silver_user_activity"
GOLD_TABLE = f"{CATALOG}.{SCHEMA_GOLD}.real_time_fraud_triage"
BATCH_LIMIT = 50

# COMMAND ----------

df = spark.table(SILVER_TABLE).limit(BATCH_LIMIT)
rows = df.collect()

# COMMAND ----------

def risk_to_action(risk_level: str, risk_score: float) -> str:
    if risk_level == "CRITICAL" or risk_score >= 85:
        return "BLOCK"
    if risk_level == "HIGH" or risk_score >= 70:
        return "CHALLENGE"
    if risk_level == "MEDIUM" or risk_score >= 50:
        return "MONITOR"
    return "APPROVE"

# COMMAND ----------

gold_rows = []
for row in rows:
    txn = row.asDict()
    try:
        out = run_reasoning_agent(txn)
        action = risk_to_action(out.get("risk_level", ""), out.get("risk_score", 0))
        reason = out.get("reason", "")
        # Lakebase (operational triage)
        upsert_triage(
            transaction_id=txn["transaction_id"],
            user_id=txn["user_id"],
            risk_score=float(out.get("risk_score", 0)),
            risk_level=out.get("risk_level", "LOW"),
            automated_action=action,
            action_reason=reason,
        )
        # Build row for gold Delta (Genie views)
        gold_rows.append(Row(
            transaction_id=txn["transaction_id"],
            user_id=txn["user_id"],
            risk_score=float(out.get("risk_score", 0)),
            risk_level=out.get("risk_level", "LOW"),
            automated_action=action,
            action_reason=reason,
            action_timestamp=None,
            review_status="PENDING",
            reviewed_by=None,
            review_timestamp=None,
            created_at=None,
        ))
    except Exception as e:
        upsert_triage(txn["transaction_id"], txn["user_id"], 0, "LOW", "MONITOR", f"Error: {e}")
        gold_rows.append(Row(transaction_id=txn["transaction_id"], user_id=txn["user_id"], risk_score=0.0, risk_level="LOW", automated_action="MONITOR", action_reason=str(e), action_timestamp=None, review_status="PENDING", reviewed_by=None, review_timestamp=None, created_at=None))

# COMMAND ----------

# Write to UC gold table (merge so view v_false_positive_ratio has data)
if gold_rows:
    from delta.tables import DeltaTable
    df_gold = spark.createDataFrame(gold_rows)
    df_gold = df_gold.withColumn("action_timestamp", current_timestamp()).withColumn("created_at", current_timestamp())
    if spark.catalog.tableExists(GOLD_TABLE):
        dt = DeltaTable.forName(spark, GOLD_TABLE)
        dt.alias("t").merge(
            df_gold.alias("s"),
            "t.transaction_id = s.transaction_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        df_gold.write.format("delta").mode("append").saveAsTable(GOLD_TABLE)
    print(f"Updated {GOLD_TABLE} with {len(gold_rows)} rows.")
print("Batch complete. Lakebase + UC gold updated. Genie v_false_positive_ratio can use gold table.")
