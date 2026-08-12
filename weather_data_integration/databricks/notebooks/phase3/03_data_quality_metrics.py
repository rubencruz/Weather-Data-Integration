# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 3 - Data quality, quarantine and metrics
# MAGIC #
# MAGIC # Produces valid/invalid datasets plus an auditable quality metrics table.

# COMMAND ----------

from pyspark.sql import functions as F
from uuid import uuid4

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "weather_dev")
CATALOG = dbutils.widgets.get("catalog") or "main"
SCHEMA = dbutils.widgets.get("schema") if dbutils.widgets.get("schema") else "weather_dev"
SILVER = f"{CATALOG}.{SCHEMA}.weather_silver"
QUARANTINE = f"{CATALOG}.{SCHEMA}.weather_quarantine"
QUALITY = f"{CATALOG}.{SCHEMA}.weather_quality_metrics"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {QUARANTINE} (
    city STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    observation_time TIMESTAMP,
    temperature_c DOUBLE,
    humidity_pct DOUBLE,
    wind_speed_kmh DOUBLE,
    precipitation_mm DOUBLE,
    snapshot_id STRING,
    quality_failure STRING,
    quality_failure_at TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {QUALITY} (
    quality_run_id STRING,
    run_timestamp TIMESTAMP,
    total_records BIGINT,
    valid_records BIGINT,
    invalid_records BIGINT,
    quality_score DOUBLE
) USING DELTA
""")

df = spark.table(SILVER)

valid_condition = (
    F.col("city").isNotNull()
    & F.col("observation_time").isNotNull()
    & F.col("temperature_c").isNotNull()
    & F.col("humidity_pct").between(0, 100)
    & (F.col("wind_speed_kmh") >= 0)
    & (F.col("precipitation_mm") >= 0)
)

invalid = (
    df.filter(~valid_condition)
    .withColumn(
        "quality_failure",
        F.when(F.col("city").isNull(), "CITY_NULL")
        .when(F.col("observation_time").isNull(), "OBSERVATION_TIME_NULL")
        .when(F.col("temperature_c").isNull(), "TEMPERATURE_NULL")
        .when(~F.col("humidity_pct").between(0, 100), "HUMIDITY_OUT_OF_RANGE")
        .when(F.col("wind_speed_kmh") < 0, "WIND_SPEED_NEGATIVE")
        .when(F.col("precipitation_mm") < 0, "PRECIPITATION_NEGATIVE")
        .otherwise("UNKNOWN")
    )
    .withColumn("quality_failure_at", F.current_timestamp())
)

# Keep the quarantine idempotent by replacing it with the current complete invalid set.
# O overwriteSchema limpa a estrutura antiga e cria a nova estrutura baseada no DataFrame 'invalid'
invalid.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(QUARANTINE)

total = df.count()
invalid_count = invalid.count()
valid_count = total - invalid_count
score = 1.0 if total == 0 else valid_count / total

metrics = spark.createDataFrame([
    (str(uuid4()), total, valid_count, invalid_count, float(score))
], ["quality_run_id", "total_records", "valid_records", "invalid_records", "quality_score"])
metrics = metrics.withColumn("run_timestamp", F.current_timestamp())
metrics.write.mode("append").format("delta").saveAsTable(QUALITY)

print(f"Total={total} Valid={valid_count} Invalid={invalid_count} Score={score:.2%}")
display(spark.table(QUALITY).orderBy(F.col("run_timestamp").desc()))
