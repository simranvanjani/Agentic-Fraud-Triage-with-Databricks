# Databricks notebook source
# MAGIC %pip install databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Vector Search: Endpoint + Fraud Signatures Index (Agent Bricks)

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

CATALOG = "Financial_Security"
SCHEMA = "silver"
SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.fraud_signatures"
INDEX_NAME = f"{CATALOG}.{SCHEMA}.fraud_signatures_index"
ENDPOINT_NAME = "fraud-detection-vs-endpoint"
EMBEDDING_MODEL = "databricks-bge-large-en"

# COMMAND ----------

client = VectorSearchClient()
try:
    client.create_endpoint(name=ENDPOINT_NAME, endpoint_type="STANDARD")
except Exception as e:
    if "already exists" not in str(e).lower():
        raise
client.create_delta_sync_index(
    endpoint_name=ENDPOINT_NAME,
    source_table_name=SOURCE_TABLE,
    index_name=INDEX_NAME,
    pipeline_type="TRIGGERED",
    primary_key="signature_id",
    embedding_source_column="description",
    embedding_model_endpoint_name=EMBEDDING_MODEL,
    columns_to_sync=["signature_id", "signature_type", "description", "risk_weight"],
)
print(f"Index: {INDEX_NAME}. Sync and wait until Ready.")
