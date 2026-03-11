# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Delta Table: real_time_fraud_triage (UC)
# MAGIC Mirror of triage data in Unity Catalog so Genie views (e.g. False Positive Ratio) can read from it.
# MAGIC Populate via batch job (13) or sync from Lakebase.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

CATALOG = "Financial_Security"
SCHEMA = "gold"
TABLE = "real_time_fraud_triage"

# COMMAND ----------

# Match columns used by v_false_positive_ratio: review_status, automated_action, etc.
schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("risk_score", DoubleType(), False),
    StructField("risk_level", StringType(), False),
    StructField("automated_action", StringType(), False),
    StructField("action_reason", StringType(), True),
    StructField("action_timestamp", TimestampType(), True),
    StructField("review_status", StringType(), True),
    StructField("reviewed_by", StringType(), True),
    StructField("review_timestamp", TimestampType(), True),
    StructField("created_at", TimestampType(), True),
])

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# Create empty table (batch job 13 will merge/append; or sync from Lakebase)
spark.createDataFrame([], schema).write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.{TABLE}")
print(f"Created: {CATALOG}.{SCHEMA}.{TABLE}")
