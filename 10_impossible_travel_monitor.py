# Databricks notebook source
# MAGIC %md
# MAGIC # Impossible Travel Monitor
# MAGIC Flags users whose geolocation jumps >500 miles in 10 minutes (Databricks Connect compatible).

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import col, lag, unix_timestamp, when, round as spark_round
import math

CATALOG = "Financial_Security"
SCHEMA_RAW = "raw"
SCHEMA_SILVER = "silver"
MILES_THRESHOLD = 500
MINUTES_THRESHOLD = 10

def haversine_miles(lat1, lon1, lat2, lon2):
    """Approximate distance in miles between two (lat, lon) points."""
    R = 3959
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

# COMMAND ----------

# UDF for distance in miles
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
def _miles(lat1, lon1, lat2, lon2):
    if None in (lat1, lon1, lat2, lon2):
        return None
    return haversine_miles(float(lat1), float(lon1), float(lat2), float(lon2))
miles_between = udf(_miles, DoubleType())

# COMMAND ----------

# Read login stream (from bronze or silver)
df = spark.table(f"{CATALOG}.{SCHEMA_RAW}.bronze_login_logs").withColumn(
    "login_time", col("timestamp").cast("timestamp")
).withColumn("lat", col("latitude").cast("double")).withColumn("lon", col("longitude").cast("double"))

window = Window.partitionBy("user_id").orderBy("login_time")
df_prev = df.withColumn("prev_lat", lag("lat").over(window)).withColumn("prev_lon", lag("lon").over(window)).withColumn("prev_time", lag("login_time").over(window))

df_prev = df_prev.withColumn("minutes_diff", (unix_timestamp(col("login_time")) - unix_timestamp(col("prev_time"))) / 60)
df_prev = df_prev.withColumn("miles_diff", miles_between(col("prev_lat"), col("prev_lon"), col("lat"), col("lon")))

impossible_travel = df_prev.filter(
    col("minutes_diff").isNotNull() & (col("minutes_diff") <= MINUTES_THRESHOLD) & (col("miles_diff") >= MILES_THRESHOLD)
).select(
    col("user_id"),
    col("login_time"),
    col("prev_time"),
    spark_round(col("minutes_diff"), 2).alias("minutes_between_logins"),
    spark_round(col("miles_diff"), 2).alias("miles_traveled"),
    col("ip_address"),
    col("city"),
)

impossible_travel.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA_SILVER}.impossible_travel_flags")
print(f"Impossible travel flags: {CATALOG}.{SCHEMA_SILVER}.impossible_travel_flags")
display(impossible_travel)
