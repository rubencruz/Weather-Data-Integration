# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 2 - Silver transformation
# MAGIC Parse Bronze JSON, standardize types and remove duplicate observations.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DoubleType

bronze_table = "main.weather_dev.weather_bronze"
silver_table = "main.weather_dev.weather_silver"

schema = StructType([
    StructField("hourly", StructType([
        StructField("time", ArrayType(StringType())),
        StructField("temperature_2m", ArrayType(DoubleType())),
        StructField("relative_humidity_2m", ArrayType(DoubleType())),
        StructField("wind_speed_10m", ArrayType(DoubleType())),
        StructField("precipitation", ArrayType(DoubleType())),
    ]))
])

bronze = spark.table(bronze_table)

parsed = bronze.withColumn("payload", F.from_json("raw_payload", schema))

silver = (
    parsed
    .select(
        "city", "latitude", "longitude",
        F.explode(
            F.arrays_zip(
                "payload.hourly.time",
                "payload.hourly.temperature_2m",
                "payload.hourly.relative_humidity_2m",
                "payload.hourly.wind_speed_10m",
                "payload.hourly.precipitation",
            )
        ).alias("x")
    )
    .select(
        "city", "latitude", "longitude",
        F.to_timestamp("x.time").alias("observation_time"),
        F.col("x.temperature_2m").alias("temperature_c"),
        F.col("x.relative_humidity_2m").alias("humidity_pct"),
        F.col("x.wind_speed_10m").alias("wind_speed_kmh"),
        F.col("x.precipitation").alias("precipitation_mm"),
    )
    .dropDuplicates(["city", "observation_time"])
)

silver.write.mode("overwrite").format("delta").saveAsTable(silver_table)
display(silver)
