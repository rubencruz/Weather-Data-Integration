# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 2 - Data Quality
# MAGIC Split valid records from quarantine records.

# COMMAND ----------

from pyspark.sql import functions as F

silver_table = "main.weather_dev.weather_silver"
quarantine_table = "main.weather_dev.weather_quarantine"

df = spark.table(silver_table)

condition = (
    F.col("city").isNotNull()
    & F.col("observation_time").isNotNull()
    & F.col("temperature_c").isNotNull()
    & F.col("humidity_pct").between(0, 100)
    & (F.col("wind_speed_kmh") >= 0)
    & (F.col("precipitation_mm") >= 0)
)

valid = df.filter(condition)
invalid = df.filter(~condition).withColumn("quality_failure_at", F.current_timestamp())

invalid.write.mode("overwrite").format("delta").saveAsTable(quarantine_table)

print("valid:", valid.count())
print("quarantine:", invalid.count())

display(invalid)
