# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Mock Data from Volume to Bronze
# MAGIC Reads CSV from `Financial_Security.raw.mock_source_data` into Bronze Delta tables.

# COMMAND ----------

CATALOG = "Financial_Security"
SCHEMA_RAW = "raw"
SCHEMA_SILVER = "silver"
VOLUME = "mock_source_data"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA_RAW}/{VOLUME}"

# COMMAND ----------

# Read transactions from volume
df_txn = spark.read.option("header", True).csv(f"{VOLUME_PATH}/transactions.csv")
df_txn = df_txn.withColumn("amount", df_txn["amount"].cast("double"))
df_txn.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA_RAW}.bronze_transactions")
print(f"Bronze: {CATALOG}.{SCHEMA_RAW}.bronze_transactions")

# COMMAND ----------

# Read login logs from volume
df_log = spark.read.option("header", True).csv(f"{VOLUME_PATH}/login_logs.csv")
df_log = df_log.withColumn("latitude", df_log["latitude"].cast("double")).withColumn("longitude", df_log["longitude"].cast("double"))
df_log.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA_RAW}.bronze_login_logs")
print(f"Bronze: {CATALOG}.{SCHEMA_RAW}.bronze_login_logs")
