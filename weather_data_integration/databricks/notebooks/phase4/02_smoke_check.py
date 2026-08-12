# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 4 - Production smoke test
# MAGIC #
# MAGIC # Validates the final Delta Gold dataset after deployment.

# COMMAND ----------

from pyspark.sql import functions as F

catalog = dbutils.widgets.get("catalog") if dbutils.widgets.get("catalog") else "main"
schema = dbutils.widgets.get("schema") or "weather_dev"
gold = f"{catalog}.{schema}.weather_daily_gold"

df = spark.table(gold)
required = [
    "city", "observation_date", "avg_temperature_c", "min_temperature_c",
    "max_temperature_c", "avg_humidity_pct", "total_precipitation_mm",
    "avg_wind_speed_kmh", "record_count", "processed_at"
]
missing = [c for c in required if c not in df.columns]
if missing:
    raise AssertionError(f"Missing Gold columns: {missing}")

if df.limit(1).count() == 0:
    raise AssertionError("Gold table is empty")

if df.filter(F.col("record_count") <= 0).limit(1).count() > 0:
    raise AssertionError("Gold contains non-positive record_count")

print(f"Smoke test OK: {gold} rows={df.count()}")
