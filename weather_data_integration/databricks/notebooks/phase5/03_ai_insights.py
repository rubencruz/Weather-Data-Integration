# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 5 - 03 AI Insights
# MAGIC #
# MAGIC # Combines AI analysis with deterministic features to create a city-level insight product.

# COMMAND ----------

from pyspark.sql import functions as F


dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "weather_dev")
CATALOG = dbutils.widgets.get("catalog") or "main"
SCHEMA = dbutils.widgets.get("schema") or "weather_dev"

FEATURES = f"{CATALOG}.{SCHEMA}.weather_ai_features"
ANALYSIS = f"{CATALOG}.{SCHEMA}.weather_ai_analysis"
INSIGHTS = f"{CATALOG}.{SCHEMA}.weather_ai_insights"

# COMMAND ----------

features = spark.table(FEATURES)
analysis = spark.table(ANALYSIS)

joined = features.join(analysis.select(
    "ai_feature_id", "ai_risk", "llm_insight", "ai_status", "ai_model", "prompt_version","deterministic_analysis"
), on="ai_feature_id", how="left")

insights = (
    joined
    .withColumn(
        "insight_type",
        F.when(F.col("temperature_anomaly_flag"), "TEMPERATURE_ANOMALY")
         .when(F.col("high_wind_flag") & F.col("precipitation_flag"), "WIND_AND_PRECIPITATION")
         .when(F.col("high_wind_flag"), "HIGH_WIND")
         .when(F.col("precipitation_flag"), "PRECIPITATION")
         .otherwise("NORMAL")
    )
    .withColumn(
        "insight_text",
        F.coalesce(
            F.col("llm_insight"),
            F.col("deterministic_analysis")
        )
    )
    .withColumn(
        "insight_priority",
        F.when(F.col("ai_risk") == "HIGH", 3)
         .when(F.col("ai_risk") == "MODERATE", 2)
         .when(F.col("ai_risk") == "LOW", 1)
         .otherwise(0)
    )
    .withColumn(
        "insight_id",
        F.sha2(F.concat_ws("||", "ai_feature_id", "insight_type"), 256)
    )
    .withColumn("insight_version", F.lit("v1"))
    .withColumn("insight_generated_at", F.current_timestamp())
    .select(
        "insight_id", "ai_feature_id", "city", "observation_time", "observation_date",
        "insight_type", "insight_priority", "deterministic_risk", "ai_risk",
        "weather_severity_score", "temperature_anomaly_flag", "high_wind_flag",
        "precipitation_flag", "insight_text", "ai_status", "ai_model", "prompt_version",
        "insight_version", "insight_generated_at"
    )
)

(insights.write.format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(INSIGHTS))

print(f"AI insight rows: {spark.table(INSIGHTS).count()}")
display(spark.table(INSIGHTS).orderBy(F.col("insight_priority").desc(), F.col("observation_time").desc()))
