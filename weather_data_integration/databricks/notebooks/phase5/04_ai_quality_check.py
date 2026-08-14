# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 5 - 04 AI Quality Check
# MAGIC #
# MAGIC # Validates source data, AI features, AI outputs and deterministic/AI consistency.

# COMMAND ----------

from pyspark.sql import functions as F


dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "weather_dev")
CATALOG = dbutils.widgets.get("catalog") or "main"
SCHEMA = dbutils.widgets.get("schema") or "weather_dev"

FEATURES = f"{CATALOG}.{SCHEMA}.weather_ai_features"
ANALYSIS = f"{CATALOG}.{SCHEMA}.weather_ai_analysis"
INSIGHTS = f"{CATALOG}.{SCHEMA}.weather_ai_insights"
QUALITY = f"{CATALOG}.{SCHEMA}.weather_ai_quality"

# COMMAND ----------

f = spark.table(FEATURES).select(
    "ai_feature_id", "city", "observation_time", "temperature_c", "humidity_pct",
    "wind_speed_kmh", "precipitation_mm", "weather_severity_score", "deterministic_risk",
    "feature_version"
)
a = spark.table(ANALYSIS).select(
    "ai_feature_id", "ai_risk", "llm_insight", "ai_status", "ai_model", "prompt_version"
)
i = spark.table(INSIGHTS).select(
    "ai_feature_id", "insight_id", "insight_type", "insight_text", "insight_version"
)

quality = (
    f.join(a, "ai_feature_id", "left")
     .join(i, "ai_feature_id", "left")
     .withColumn(
         "source_valid",
         F.col("city").isNotNull()
         & F.col("observation_time").isNotNull()
         & F.col("temperature_c").isNotNull()
         & F.col("humidity_pct").between(0, 100)
         & (F.col("wind_speed_kmh") >= 0)
         & (F.col("precipitation_mm") >= 0)
     )
     .withColumn(
         "feature_valid",
         F.col("ai_feature_id").isNotNull()
         & F.col("feature_version").isNotNull()
         & F.col("weather_severity_score").isNotNull()
         & F.col("deterministic_risk").isin("NORMAL", "LOW", "MODERATE", "HIGH")
     )
     .withColumn(
         "ai_output_valid",
         F.col("ai_risk").isin("NORMAL", "LOW", "MODERATE", "HIGH")
         & F.col("ai_model").isNotNull()
         & F.col("prompt_version").isNotNull()
         & F.col("insight_text").isNotNull()
         & (F.length(F.trim(F.col("insight_text"))) >= 20)
     )
     .withColumn(
         "ai_consistency",
         F.when(F.col("ai_status") == "DETERMINISTIC", "PASS")
          .when(F.col("ai_risk") == F.col("deterministic_risk"), "PASS")
          .otherwise("REVIEW")
     )
     .withColumn(
         "quality_passed",
         F.col("source_valid")
         & F.col("feature_valid")
         & F.col("ai_output_valid")
         & F.col("insight_id").isNotNull()
         & F.col("insight_version").isNotNull()
         & (F.col("ai_consistency").isin("PASS", "REVIEW"))
     )
     .withColumn("quality_checked_at", F.current_timestamp())
)

(quality.write.format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(QUALITY))

metrics = quality.agg(
    F.count("*").alias("total_records"),
    F.sum(F.when(F.col("quality_passed"), 1).otherwise(0)).alias("passed"),
    F.sum(F.when(~F.col("quality_passed"), 1).otherwise(0)).alias("failed"),
    F.sum(F.when(F.col("ai_consistency") == "REVIEW", 1).otherwise(0)).alias("review_records")
).collect()[0]

print(f"AI quality total: {metrics['total_records']}")
print(f"AI quality passed: {metrics['passed'] or 0}")
print(f"AI quality failed: {metrics['failed'] or 0}")
print(f"AI consistency review: {metrics['review_records'] or 0}")

# REVIEW means human/engineering inspection may be useful; it is not a pipeline failure.
if (metrics["failed"] or 0) > 0:
    raise RuntimeError(f"AI quality gate failed: {metrics['failed']} record(s).")

display(quality.orderBy(F.col("quality_passed").asc(), F.col("ai_consistency").desc()))
