# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 1 - Transform and publish
# MAGIC
# MAGIC Normalize hourly arrays and optionally write to PostgreSQL through JDBC.
# MAGIC
# MAGIC Set `JDBC_URL`, `JDBC_USER`, and `JDBC_PASSWORD` in the notebook/job environment.
# MAGIC Never hard-code credentials.

# COMMAND ----------

from pyspark.sql import functions as F

# If the previous notebook is not attached to the same workflow, replace this
# with a read from a persisted Bronze Delta table.
cities = [
    ("Brasilia", -15.793889, -47.882778),
    ("Sao Paulo", -23.55052, -46.633308),
    ("Rio de Janeiro", -22.906847, -43.172896),
]

# Example expected raw_df shape:
# city, latitude, longitude, hourly.time, hourly.temperature_2m, ...

weather_df = (
    raw_df
    .select(
        "city", "latitude", "longitude",
        F.explode(
            F.arrays_zip(
                "hourly.time",
                "hourly.temperature_2m",
                "hourly.relative_humidity_2m",
                "hourly.wind_speed_10m",
                "hourly.precipitation",
            )
        ).alias("x")
    )
    .select(
        "city", "latitude", "longitude",
        F.to_timestamp("x.time").alias("observation_time"),
        F.col("x.temperature_2m").cast("double").alias("temperature_c"),
        F.col("x.relative_humidity_2m").cast("double").alias("humidity_pct"),
        F.col("x.wind_speed_10m").cast("double").alias("wind_speed_kmh"),
        F.col("x.precipitation").cast("double").alias("precipitation_mm"),
    )
)

display(weather_df)

# COMMAND ----------

# Optional PostgreSQL write.
# Configure these as environment variables/secrets before enabling.
import os

jdbc_url = os.getenv("WEATHER_JDBC_URL")
jdbc_user = os.getenv("WEATHER_JDBC_USER")
jdbc_password = os.getenv("WEATHER_JDBC_PASSWORD")

if jdbc_url and jdbc_user and jdbc_password:
    (
        weather_df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "weather_observation")
        .option("user", jdbc_user)
        .option("password", jdbc_password)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )
else:
    print("JDBC target not configured; transformation completed without publishing.")
