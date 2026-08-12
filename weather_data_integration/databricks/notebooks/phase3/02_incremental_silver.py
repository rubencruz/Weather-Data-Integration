# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 3 - Incremental Silver transformation
# MAGIC #
# MAGIC # Reads only new Bronze snapshots since the last Silver load and upserts observations into Delta.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DoubleType
from delta.tables import DeltaTable

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "weather_dev")
CATALOG = dbutils.widgets.get("catalog") or "main"
SCHEMA = dbutils.widgets.get("schema") if dbutils.widgets.get("schema") else "weather_dev"
BRONZE = f"{CATALOG}.{SCHEMA}.weather_bronze"
SILVER = f"{CATALOG}.{SCHEMA}.weather_silver"

schema = StructType([
    StructField("hourly", StructType([
        StructField("time", ArrayType(StringType())),
        StructField("temperature_2m", ArrayType(DoubleType())),
        StructField("relative_humidity_2m", ArrayType(DoubleType())),
        StructField("wind_speed_10m", ArrayType(DoubleType())),
        StructField("precipitation", ArrayType(DoubleType())),
    ]))
])

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER} (
    city STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    observation_time TIMESTAMP,
    temperature_c DOUBLE,
    humidity_pct DOUBLE,
    wind_speed_kmh DOUBLE,
    precipitation_mm DOUBLE,
    snapshot_id STRING,
    processed_at TIMESTAMP
) USING DELTA
""")

bronze = spark.table(BRONZE)
processed_snapshots = spark.table(SILVER).select("snapshot_id").distinct()
new_bronze = bronze.join(processed_snapshots, "snapshot_id", "left_anti")

parsed = new_bronze.withColumn("payload", F.from_json("raw_payload", schema))

silver_incoming = (
    parsed
    .select(
        "city", "latitude", "longitude", "snapshot_id",
        F.explode(F.arrays_zip(
            "payload.hourly.time",
            "payload.hourly.temperature_2m",
            "payload.hourly.relative_humidity_2m",
            "payload.hourly.wind_speed_10m",
            "payload.hourly.precipitation",
        )).alias("x")
    )
    .select(
        "city", "latitude", "longitude", "snapshot_id",
        F.to_timestamp("x.time").alias("observation_time"),
        F.col("x.temperature_2m").alias("temperature_c"),
        F.col("x.relative_humidity_2m").alias("humidity_pct"),
        F.col("x.wind_speed_10m").alias("wind_speed_kmh"),
        F.col("x.precipitation").alias("precipitation_mm"),
    )
    .withColumn("processed_at", F.current_timestamp())
    .dropDuplicates(["city", "observation_time"])
)

if silver_incoming.take(1):
    target = DeltaTable.forName(spark, SILVER)
    (
        target.alias("t")
        .merge(
            silver_incoming.alias("s"),
            "t.city = s.city AND t.observation_time = s.observation_time"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

print(f"New Bronze snapshots: {new_bronze.count()}")
print(f"Silver total rows: {spark.table(SILVER).count()}")
display(spark.table(SILVER))
