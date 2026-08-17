# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 7 - Operational metadata catalog
# MAGIC #
# MAGIC # Creates a lightweight, project-local metadata catalog for the weather
# MAGIC # data product. It records table/column definitions, ownership and layer.

# COMMAND ----------

from pyspark.sql import functions as F


dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "weather_dev")
CATALOG = dbutils.widgets.get("catalog") or "main"
SCHEMA = dbutils.widgets.get("schema") or "weather_dev"
METADATA = f"{CATALOG}.{SCHEMA}.weather_data_catalog"

TABLES = {
    "weather_bronze": ("bronze", "Raw Open-Meteo snapshots; source of truth for reprocessing."),
    "weather_silver": ("silver", "Validated and normalized hourly weather observations."),
    "weather_quarantine": ("silver", "Records that fail data quality validation."),
    "weather_quality_metrics": ("quality", "Historical data quality measurements."),
    "weather_quality_gate": ("quality", "Quality gate decisions that control downstream processing."),
    "weather_daily_gold": ("gold", "Daily weather aggregates for analytics."),
    "weather_ai_features": ("ai", "Feature dataset used by anomaly detection."),
    "weather_ai_anomalies": ("ai", "Explainable weather anomaly scores."),
    "weather_ai_insights": ("ai", "City-level operational weather insights."),
}

rows = []
for table_name, (layer, description) in TABLES.items():
    full_name = f"{CATALOG}.{SCHEMA}.{table_name}"
    if spark.catalog.tableExists(full_name):
        for field in spark.table(full_name).schema.fields:
            rows.append((
                full_name,
                table_name,
                layer,
                field.name,
                field.dataType.simpleString(),
                field.nullable,
                description,
            ))

if rows:
    metadata_df = spark.createDataFrame(rows, [
        "table_name", "dataset", "layer", "column_name", "data_type",
        "nullable", "table_description"
    ]).withColumn("catalog", F.lit(CATALOG)).withColumn("schema_name", F.lit(SCHEMA))
    metadata_df = metadata_df.withColumn("updated_at", F.current_timestamp())
    metadata_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(METADATA)

print(f"Metadata catalog created: {METADATA}; columns documented: {len(rows)}")
if rows:
    display(spark.table(METADATA).orderBy("layer", "dataset", "column_name"))
