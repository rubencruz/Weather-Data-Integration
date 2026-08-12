# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 2 - Gold aggregation and PostgreSQL publication

# COMMAND ----------

from pyspark.sql import functions as F
import os

silver_table = "main.weather_dev.weather_silver"
gold_table = "main.weather_dev.weather_daily_gold"

df = spark.table(silver_table)

valid = df.filter(
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

gold.write.mode("overwrite").format("delta").saveAsTable(gold_table)
display(gold)

# JDBC publication.
jdbc_url = os.getenv("WEATHER_JDBC_URL")
jdbc_user = os.getenv("WEATHER_JDBC_USER")
jdbc_password = os.getenv("WEATHER_JDBC_PASSWORD")

if jdbc_url and jdbc_user and jdbc_password:
    (
        gold.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "weather_daily_gold")
        .option("user", jdbc_user)
        .option("password", jdbc_password)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )
else:
    print("JDBC target not configured; Gold remains in Delta.")
