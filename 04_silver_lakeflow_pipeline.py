# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Feature Engineering + Wire/IP Rule
# MAGIC Joins transactions and login_logs; flags wire transfers >$10k within 1 hour of IP change.
# MAGIC Can be run as a one-off job or wired to Lakeflow (triggered/continuous).

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import (
    col, to_timestamp, when, lag, row_number,
    datediff, unix_timestamp, round as spark_round,
)

CATALOG = "Financial_Security"
SCHEMA_RAW = "raw"
SCHEMA_SILVER = "silver"

# COMMAND ----------

# Read Bronze
df_txn = spark.table(f"{CATALOG}.{SCHEMA_RAW}.bronze_transactions").withColumn(
    "transaction_time", to_timestamp(col("timestamp"))
)
df_log = spark.table(f"{CATALOG}.{SCHEMA_RAW}.bronze_login_logs").withColumn(
    "login_time", to_timestamp(col("timestamp"))
).withColumn("mfa_changed", (col("mfa_change_flag") == "True"))

# COMMAND ----------

# IP change per user
window_ip = Window.partitionBy("user_id").orderBy("login_time")
df_ip = df_log.withColumn("prev_ip", lag("ip_address").over(window_ip)).withColumn(
    "ip_changed", when(col("prev_ip").isNotNull() & (col("ip_address") != col("prev_ip")), True).otherwise(False)
).filter(col("ip_changed")).select(
    col("user_id").alias("ip_user_id"),
    col("ip_address").alias("new_ip"),
    col("login_time").alias("ip_change_time"),
    col("latitude").alias("ip_lat"),
    col("longitude").alias("ip_lon"),
)

# MFA change (last 24h)
window_mfa = Window.partitionBy("user_id").orderBy(col("login_time").desc())
df_mfa = df_log.filter(col("mfa_changed")).withColumn("rn", row_number().over(window_mfa)).filter(col("rn") == 1).select(
    col("user_id").alias("mfa_user_id"),
    col("login_time").alias("last_mfa_change"),
)

# COMMAND ----------

# Silver: join transactions with logins (latest login per user for context)
df_log_agg = df_log.withColumn("rn", row_number().over(Window.partitionBy("user_id").orderBy(col("login_time").desc()))).filter(col("rn") == 1).select(
    col("user_id").alias("log_user_id"),
    col("ip_address"),
    col("device_id"),
    col("login_time").alias("last_login_time"),
)
silver = df_txn.join(df_log_agg, df_txn["user_id"] == df_log_agg["log_user_id"], "left").drop("log_user_id")
silver = silver.join(df_ip, (silver["user_id"] == df_ip["ip_user_id"]) & (silver["transaction_time"] >= df_ip["ip_change_time"]), "left").drop("ip_user_id")
silver = silver.join(df_mfa, silver["user_id"] == df_mfa["mfa_user_id"], "left").drop("mfa_user_id")

# COMMAND ----------

# Minutes since IP change; wire >10k within 1h of IP change
silver = silver.withColumn(
    "minutes_since_ip_change",
    when(col("ip_change_time").isNotNull(), spark_round((unix_timestamp(col("transaction_time")) - unix_timestamp(col("ip_change_time"))) / 60, 2)).otherwise(None)
).withColumn(
    "ip_changed_recently",
    when(col("ip_change_time").isNotNull() & (col("minutes_since_ip_change") <= 60), True).otherwise(False)
).withColumn(
    "mfa_change_flag",
    when(col("last_mfa_change").isNotNull() & (datediff(col("transaction_time"), col("last_mfa_change")) <= 1), True).otherwise(False)
).withColumn(
    "is_wire_transfer",
    (col("transaction_type") == "wire")
).withColumn(
    "high_value_wire",
    (col("transaction_type") == "wire") & (col("amount") >= 10000)
).withColumn(
    "wire_over_10k_within_1h_ip_change",
    (col("high_value_wire") & col("ip_changed_recently"))
)

# COMMAND ----------

silver_user_activity = silver.select(
    col("transaction_id"),
    col("user_id"),
    col("amount"),
    col("merchant_id"),
    col("transaction_type"),
    col("transaction_time"),
    col("ip_address"),
    col("device_id"),
    col("ip_changed_recently"),
    col("minutes_since_ip_change"),
    col("mfa_change_flag"),
    col("high_value_wire"),
    col("wire_over_10k_within_1h_ip_change"),
    col("ip_lat").alias("ip_latitude"),
    col("ip_lon").alias("ip_longitude"),
)

silver_user_activity.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA_SILVER}.silver_user_activity")
print(f"Silver: {CATALOG}.{SCHEMA_SILVER}.silver_user_activity")
display(spark.table(f"{CATALOG}.{SCHEMA_SILVER}.silver_user_activity").limit(20))
