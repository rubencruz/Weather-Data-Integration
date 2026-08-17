# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 5 - AI anomaly detection
# MAGIC #
# MAGIC # Uses explainable statistical anomaly scoring over the engineered features.

# COMMAND ----------

from pyspark.sql import functions as F


dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "weather_dev")
CATALOG = dbutils.widgets.get("catalog") or "main"
SCHEMA = dbutils.widgets.get("schema") or "weather_dev"
FEATURES = f"{CATALOG}.{SCHEMA}.weather_ai_features"
ANOMALIES = f"{CATALOG}.{SCHEMA}.weather_ai_anomalies"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ANOMALIES} (
    city STRING,
    observation_time TIMESTAMP,
    temperature_c DOUBLE,
    humidity_pct DOUBLE,
    wind_speed_kmh DOUBLE,
    precipitation_mm DOUBLE,
    temperature_zscore DOUBLE,
    anomaly_score DOUBLE,
    anomaly_type STRING,
    severity STRING,
    detected_at TIMESTAMP
) USING DELTA
""")

# The rolling standard deviation is already calculated from recent observations.
# A small epsilon prevents division by zero.
df = spark.table(FEATURES)
scored = (
    df
    .withColumn(
        "temperature_zscore",
        F.when(F.col("rolling_std_temperature_c") > 0,
               (F.col("temperature_c") - F.col("rolling_avg_temperature_c")) / F.col("rolling_std_temperature_c"))
         .otherwise(F.lit(0.0))
    )
    .withColumn(
        "anomaly_score",
        F.greatest(
            F.abs(F.coalesce(F.col("temperature_zscore"), F.lit(0.0))),
            F.when(F.col("high_wind_flag"), F.lit(2.0)).otherwise(F.lit(0.0)),
            F.when(F.col("precipitation_flag"), F.lit(1.5)).otherwise(F.lit(0.0)),
        )
    )
    .withColumn(
        "anomaly_type",
        F.when(F.abs(F.coalesce(F.col("temperature_zscore"), F.lit(0.0))) >= 3, "temperature_extreme")
         .when(F.col("high_wind_flag") & F.col("precipitation_flag"), "wind_and_precipitation")
         .when(F.col("high_wind_flag"), "high_wind")
         .when(F.col("precipitation_flag"), "heavy_precipitation")
         .otherwise("normal")
    )
    .withColumn(
        "severity",
        F.when(F.col("anomaly_score") >= 3, "high")
         .when(F.col("anomaly_score") >= 2, "medium")
         .when(F.col("anomaly_score") >= 1.5, "low")
         .otherwise("normal")
    )
    .withColumn("detected_at", F.current_timestamp())
    .select(
        "city", "observation_time", "temperature_c", "humidity_pct", "wind_speed_kmh",
        "precipitation_mm", "temperature_zscore", "anomaly_score", "anomaly_type",
        "severity", "detected_at"
    )
)

scored.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(ANOMALIES)

print(f"Anomaly rows: {spark.table(ANOMALIES).count()}")
display(
    spark.table(ANOMALIES)
    .filter(F.col("anomaly_type") != "normal")
    .orderBy(F.col("anomaly_score").desc(), F.col("observation_time").desc())
)
