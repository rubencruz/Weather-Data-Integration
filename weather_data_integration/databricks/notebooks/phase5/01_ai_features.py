# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 5 - 01 AI Features
# MAGIC #
# MAGIC # Converts trusted Silver weather data into an AI-ready feature layer.
# MAGIC # This notebook is deterministic: it does NOT call an LLM.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window


dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "weather_dev")
CATALOG = dbutils.widgets.get("catalog") or "main"
SCHEMA = dbutils.widgets.get("schema") or "weather_dev"

SILVER = f"{CATALOG}.{SCHEMA}.weather_silver"
FEATURES = f"{CATALOG}.{SCHEMA}.weather_ai_features"

# COMMAND ----------

base = (
    spark.table(SILVER)
    .filter(
        F.col("city").isNotNull()
        & F.col("observation_time").isNotNull()
        & F.col("temperature_c").isNotNull()
        & F.col("humidity_pct").between(0, 100)
        & (F.col("wind_speed_kmh") >= 0)
        & (F.col("precipitation_mm") >= 0)
    )
)

w_city = Window.partitionBy("city").orderBy("observation_time")
w_6 = w_city.rowsBetween(-6, 0)
w_24 = w_city.rowsBetween(-24, 0)

# COMMAND ----------

features = (
    base
    .withColumn("observation_date", F.to_date("observation_time"))
    # Raw AI features
    .withColumn("temperature_delta_c", F.col("temperature_c") - F.lag("temperature_c").over(w_city))
    .withColumn("humidity_delta_pct", F.col("humidity_pct") - F.lag("humidity_pct").over(w_city))
    # Rolling/statistical features
    .withColumn("rolling_avg_temperature_c", F.avg("temperature_c").over(w_6))
    .withColumn("rolling_std_temperature_c", F.coalesce(F.stddev_pop("temperature_c").over(w_6), F.lit(0.0)))
    .withColumn("rolling_precipitation_mm", F.sum("precipitation_mm").over(w_24))
    # Semantic features
    .withColumn(
        "precipitation_category",
        F.when(F.col("precipitation_mm") == 0, "none")
         .when(F.col("precipitation_mm") < 5, "low")
         .when(F.col("precipitation_mm") < 20, "moderate")
         .otherwise("high")
    )
    .withColumn(
        "wind_category",
        F.when(F.col("wind_speed_kmh") < 20, "low")
         .when(F.col("wind_speed_kmh") < 40, "moderate")
         .otherwise("high")
    )
    .withColumn(
        "temperature_category",
        F.when(F.col("temperature_c") >= 35, "hot")
         .when(F.col("temperature_c") <= 5, "cold")
         .otherwise("normal")
    )
    # Explainable signals
    .withColumn("precipitation_flag", F.col("precipitation_mm") >= 5.0)
    .withColumn("high_wind_flag", F.col("wind_speed_kmh") >= 40.0)
    .withColumn("temperature_zscore",
                F.when(F.col("rolling_std_temperature_c") > 0,
                       (F.col("temperature_c") - F.col("rolling_avg_temperature_c")) /
                       F.col("rolling_std_temperature_c"))
                 .otherwise(F.lit(0.0)))
    .withColumn("temperature_anomaly_flag", F.abs(F.col("temperature_zscore")) >= 3.0)
    # Lightweight proxy; deliberately not presented as an official meteorological heat index.
    .withColumn("heat_index_proxy", F.col("temperature_c") + (F.col("humidity_pct") - 50.0) * 0.03)
    # Deterministic severity signal used as a control signal for downstream AI.
    .withColumn(
        "temperature_score",
        F.when(F.col("temperature_anomaly_flag"), 3.0)
         .when((F.col("temperature_c") >= 35) | (F.col("temperature_c") <= 5), 2.0)
         .otherwise(0.0)
    )
    .withColumn("wind_score", F.when(F.col("high_wind_flag"), 2.0).otherwise(0.0))
    .withColumn("precipitation_score", F.when(F.col("precipitation_mm") >= 20, 2.0)
                                      .when(F.col("precipitation_flag"), 1.5)
                                      .otherwise(0.0))
    .withColumn(
        "weather_severity_score",
        F.greatest("temperature_score", "wind_score", "precipitation_score")
    )
    .withColumn(
        "deterministic_risk",
        F.when(F.col("weather_severity_score") >= 3, "HIGH")
         .when(F.col("weather_severity_score") >= 2, "MODERATE")
         .when(F.col("weather_severity_score") >= 1.5, "LOW")
         .otherwise("NORMAL")
    )
    .withColumn(
        "ai_context",
        F.concat_ws(
            "\n",
            F.concat(F.lit("City: "), F.col("city")),
            F.concat(F.lit("Observation time: "), F.col("observation_time").cast("string")),
            F.concat(F.lit("Temperature C: "), F.round(F.col("temperature_c"), 2)),
            F.concat(F.lit("Humidity %: "), F.round(F.col("humidity_pct"), 2)),
            F.concat(F.lit("Wind km/h: "), F.round(F.col("wind_speed_kmh"), 2)),
            F.concat(F.lit("Precipitation mm: "), F.round(F.col("precipitation_mm"), 2)),
            F.concat(F.lit("Temperature z-score: "), F.round(F.col("temperature_zscore"), 3)),
            F.concat(F.lit("Temperature anomaly: "), F.col("temperature_anomaly_flag")),
            F.concat(F.lit("Precipitation category: "), F.col("precipitation_category")),
            F.concat(F.lit("Wind category: "), F.col("wind_category")),
            F.concat(F.lit("Deterministic risk: "), F.col("deterministic_risk")),
            F.concat(F.lit("Severity score: "), F.round(F.col("weather_severity_score"), 2)),
        )
    )
    .withColumn(
        "ai_feature_id",
        F.sha2(F.concat_ws("||", "city", F.col("observation_time").cast("string")), 256)
    )
    .withColumn("feature_version", F.lit("v2"))
    .withColumn("feature_generated_at", F.current_timestamp())
)

# Full replacement is intentional: Silver is the trusted source of truth and the feature horizon is reproducible.
(features.write.format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(FEATURES))

print(f"AI feature rows: {spark.table(FEATURES).count()}")
display(spark.table(FEATURES).orderBy(F.col("observation_time").desc()))