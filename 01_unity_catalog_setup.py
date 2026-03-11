# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog: Financial_Security
# MAGIC Creates catalog, schemas, volume for mock data, and ABAC (tags) for highest-risk data access.

# COMMAND ----------

# Catalog and schemas
CATALOG = "Financial_Security"
SCHEMA_RAW = "raw"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"
VOLUME_NAME = "mock_source_data"

# COMMAND ----------

# Create catalog (run as account admin or catalog creator)
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")

# COMMAND ----------

# Create schemas
for schema in [SCHEMA_RAW, SCHEMA_SILVER, SCHEMA_GOLD]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

# COMMAND ----------

# Create volume for mock CSV/JSON (source of truth for ingestion)
spark.sql(f"""
CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA_RAW}.{VOLUME_NAME}
COMMENT 'Mock transaction and login data for fraud triage demos'
""")

# COMMAND ----------

# Grant use on catalog and schemas (customize principal for your workspace)
# spark.sql(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `your_group`")
# spark.sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{SCHEMA_RAW} TO `your_group`")
# spark.sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{SCHEMA_SILVER} TO `your_group`")
# spark.sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{SCHEMA_GOLD} TO `your_group`")
# spark.sql(f"GRANT READ VOLUME ON VOLUME {CATALOG}.{SCHEMA_RAW}.{VOLUME_NAME} TO `your_group`")

# COMMAND ----------

# ABAC: Tag for highest-risk data (PII / sensitive) - restrict who can see unmasked data
# Create tag and attach to tables once they exist (example for later)
spark.sql("""
CREATE TAG IF NOT EXISTS Financial_Security.silver.sensitivity
COMMENT 'Data sensitivity for access control'
""")
# Assign tag to column (example - run after silver table exists):
# ALTER TABLE Financial_Security.silver.silver_user_activity
#   ALTER COLUMN user_id SET TAG Financial_Security.silver.sensitivity = 'pii';

# COMMAND ----------

# PII masking: Unity Catalog column mask example (run after tables exist)
# CREATE FUNCTION Financial_Security.silver.mask_card AS (col STRING) CASE WHEN current_user() IN ('fraud_analyst_group') THEN col ELSE '****' END;
# ALTER TABLE Financial_Security.silver.silver_transactions ALTER COLUMN card_last_four SET MASK Financial_Security.silver.mask_card;

print(f"Catalog: {CATALOG}")
print(f"Schemas: {SCHEMA_RAW}, {SCHEMA_SILVER}, {SCHEMA_GOLD}")
print(f"Volume: {CATALOG}.{SCHEMA_RAW}.{VOLUME_NAME}")
