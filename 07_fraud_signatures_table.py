# Databricks notebook source
# MAGIC %md
# MAGIC # Fraud Signatures Table (Vector Search Source)
# MAGIC Known fraud signatures for Agent Bricks Vector Search: synthetic identity, mule accounts, bot-driven transfers, impossible travel.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType

CATALOG = "Financial_Security"
SCHEMA = "silver"
TABLE = "fraud_signatures"

schema = StructType([
    StructField("signature_id", StringType(), False),
    StructField("signature_type", StringType(), False),
    StructField("description", StringType(), False),
    StructField("risk_weight", DoubleType(), True),
])
data = [
    ("sig_synthetic_001", "synthetic_identity",
     "Synthetic identity: new account, rapid logins and high-value transfers, MFA change in first hours.", 0.95),
    ("sig_mule_001", "mule_account",
     "Mule account: high volume incoming/outgoing transfers, unusual geography, fast turnaround.", 0.92),
    ("sig_bot_001", "bot_driven_transfer",
     "Bot-driven: typing or device mismatch, session from one device transaction from another.", 0.88),
    ("sig_impossible_001", "impossible_travel",
     "Impossible travel: same user two impossible locations in short time, e.g. New York then Hong Kong in 30 min.", 0.98),
    ("sig_high_value_ip_001", "high_value_wire_ip_change",
     "High-value wire shortly after IP change; login from new IP within last hour.", 0.90),
    ("sig_mfa_transfer_001", "mfa_change_high_value",
     "MFA or security changed recently followed by high-value ACH or wire.", 0.85),
]

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
df = spark.createDataFrame(data, schema)
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.{TABLE}")
spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.{TABLE} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
display(spark.table(f"{CATALOG}.{SCHEMA}.{TABLE}"))
