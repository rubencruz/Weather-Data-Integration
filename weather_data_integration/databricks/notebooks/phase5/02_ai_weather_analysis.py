# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 5 - 02 AI Weather Analysis
# MAGIC #
# MAGIC # Produces a deterministic analysis and optionally an LLM classification/summary.
# MAGIC # The LLM receives only the controlled AI feature context.

# COMMAND ----------

from pyspark.sql import functions as F


dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "weather_dev")
dbutils.widgets.dropdown("enable_llm", "false", ["true", "false"])
dbutils.widgets.text("ai_endpoint", "")
CATALOG = dbutils.widgets.get("catalog") or "main"
SCHEMA = dbutils.widgets.get("schema") or "weather_dev"
ENABLE_LLM = (dbutils.widgets.get("enable_llm") or "false").lower() == "true"
AI_ENDPOINT = dbutils.widgets.get("ai_endpoint") or ""

FEATURES = f"{CATALOG}.{SCHEMA}.weather_ai_features"
ANALYSIS = f"{CATALOG}.{SCHEMA}.weather_ai_analysis"

# COMMAND ----------

df = spark.table(FEATURES)

analysis = (
    df
    .withColumn(
        "deterministic_analysis",
        F.concat(
            F.lit("Conditions for "), F.col("city"), F.lit(" are classified as "),
            F.col("deterministic_risk"), F.lit(" by deterministic rules. "),
            F.lit("Temperature is "), F.round(F.col("temperature_c"), 1), F.lit(" C, wind is "),
            F.round(F.col("wind_speed_kmh"), 1), F.lit(" km/h and precipitation is "),
            F.round(F.col("precipitation_mm"), 1), F.lit(" mm. "),
            F.when(F.col("temperature_anomaly_flag"), F.lit("A temperature anomaly was detected. "))
             .otherwise(F.lit("No temperature anomaly was detected. ")),
            F.when(F.col("high_wind_flag"), F.lit("Wind is elevated. "))
             .otherwise(F.lit("Wind is not elevated. ")),
            F.when(F.col("precipitation_flag"), F.lit("Precipitation is notable."))
             .otherwise(F.lit("Precipitation is not notable."))
        )
    )
)

if ENABLE_LLM:
    if not AI_ENDPOINT:
        raise ValueError("enable_llm=true requires ai_endpoint.")

    prompt = F.concat(
        F.lit(
            "You are a weather data analyst. Analyze ONLY the supplied structured observation. "
            "Do not invent facts or forecasts. Return a concise factual summary in 2 sentences. "
            "Also classify risk as exactly LOW, MODERATE, or HIGH. "
            "Observation:\n"
        ),
        F.col("ai_context")
    )

    # The exact endpoint/model is supplied as a parameter; no credentials are stored in source control.
    analysis = (
        analysis
        .withColumn("llm_insight", F.expr(f"ai_query('{AI_ENDPOINT}', prompt)"))
        .withColumn("ai_risk",
                    F.expr(f"upper(trim(ai_query('{AI_ENDPOINT}', concat('Classify risk as exactly LOW, MODERATE, or HIGH. Return only one label. Observation: ', ai_context)))"))
        .withColumn("ai_status", F.lit("LLM"))
    )
else:
    analysis = (
        analysis
        .withColumn("llm_insight", F.lit(None).cast("string"))
        .withColumn("ai_risk", F.col("deterministic_risk"))
        .withColumn("ai_status", F.lit("DETERMINISTIC"))
    )

analysis = (
    analysis
    .withColumn("ai_model", F.when(F.lit(ENABLE_LLM), F.lit(AI_ENDPOINT)).otherwise(F.lit("deterministic-rules-v2")))
    .withColumn("prompt_version", F.lit("weather-analysis-v1"))
    .withColumn("analyzed_at", F.current_timestamp())
    .select(
        "ai_feature_id", "city", "observation_time", "observation_date",
        "deterministic_risk", "weather_severity_score", "temperature_zscore",
        "temperature_anomaly_flag", "high_wind_flag", "precipitation_flag",
        "deterministic_analysis", "ai_risk", "llm_insight", "ai_status",
        "ai_model", "prompt_version", "analyzed_at"
    )
)

(analysis.write.format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(ANALYSIS))

print(f"AI analysis rows: {spark.table(ANALYSIS).count()}")
display(spark.table(ANALYSIS).orderBy(F.col("analyzed_at").desc()))
