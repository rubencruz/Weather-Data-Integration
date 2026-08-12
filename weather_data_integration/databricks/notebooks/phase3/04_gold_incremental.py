# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 3 - Incremental Gold aggregation
# MAGIC #
# MAGIC # Gold is maintained as a Delta table and recomputed only for affected city/date partitions.

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "weather_dev")
CATALOG = dbutils.widgets.get("catalog") or "main"
SCHEMA = dbutils.widgets.get("schema") if dbutils.widgets.get("schema") else "weather_dev"
SILVER = f"{CATALOG}.{SCHEMA}.weather_silver"
QUALITY = f"{CATALOG}.{SCHEMA}.weather_quality_metrics"
GOLD = f"{CATALOG}.{SCHEMA}.weather_daily_gold"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD} (
    city STRING,
    observation_date DATE,
    avg_temperature_c DOUBLE,
    min_temperature_c DOUBLE,
    max_temperature_c DOUBLE,
    avg_humidity_pct DOUBLE,
    total_precipitation_mm DOUBLE,
    avg_wind_speed_kmh DOUBLE,
    record_count BIGINT,
    processed_at TIMESTAMP
) USING DELTA
""")

quality = spark.table(QUALITY).orderBy(F.col("run_timestamp").desc()).limit(1).collect()
if quality and quality[0]["quality_score"] < 1.0:
    raise RuntimeError("Data quality gate failed: quality_score is below 100%.")

valid = spark.table(SILVER).filter(
    F.col("city").isNotNull()
    & F.col("observation_time").isNotNull()
    & F.col("temperature_c").isNotNull()
    & F.col("humidity_pct").between(0, 100)
    & (F.col("wind_speed_kmh") >= 0)
    & (F.col("precipitation_mm") >= 0)
)

gold = (
    valid
    .groupBy("city", F.to_date("observation_time").alias("observation_date"))
    .agg(
        F.avg("temperature_c").alias("avg_temperature_c"),
        F.min("temperature_c").alias("min_temperature_c"),
        F.max("temperature_c").alias("max_temperature_c"),
        F.avg("humidity_pct").alias("avg_humidity_pct"),
        F.sum("precipitation_mm").alias("total_precipitation_mm"),
        F.avg("wind_speed_kmh").alias("avg_wind_speed_kmh"),
        F.count("*").alias("record_count"),
    )
    .withColumn("processed_at", F.current_timestamp())
)

target = DeltaTable.forName(spark, GOLD)
(
    target.alias("t")
    .merge(
        gold.alias("s"),
        "t.city = s.city AND t.observation_date = s.observation_date"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

display(spark.table(GOLD).orderBy("observation_date", "city"))
