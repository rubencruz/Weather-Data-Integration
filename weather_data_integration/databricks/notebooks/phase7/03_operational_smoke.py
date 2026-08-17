# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 7 - Operational smoke test
# MAGIC #
# MAGIC # Validates that the production-oriented metadata and quality gate artifacts
# MAGIC # exist and contain usable information before the job is considered healthy.

# COMMAND ----------

from pyspark.sql import functions as F


dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "weather_dev")
CATALOG = dbutils.widgets.get("catalog") or "main"
SCHEMA = dbutils.widgets.get("schema") or "weather_dev"

required_tables = [
    f"{CATALOG}.{SCHEMA}.weather_bronze",
    f"{CATALOG}.{SCHEMA}.weather_silver",
    f"{CATALOG}.{SCHEMA}.weather_quality_metrics",
    f"{CATALOG}.{SCHEMA}.weather_quality_gate",
    f"{CATALOG}.{SCHEMA}.weather_daily_gold",
    f"{CATALOG}.{SCHEMA}.weather_ai_insights",
]

missing = [name for name in required_tables if not spark.catalog.tableExists(name)]
if missing:
    raise AssertionError(f"Missing production datasets: {missing}")

latest_gate = (
    spark.table(f"{CATALOG}.{SCHEMA}.weather_quality_gate")
    .orderBy(F.col("evaluated_at").desc())
    .limit(1)
)

if latest_gate.limit(1).count() == 0:
    raise AssertionError("No quality gate decision was recorded")

status = latest_gate.collect()[0]["gate_status"]
if status != "PASS":
    raise AssertionError(f"Latest data quality gate is {status}")

print("Phase 7 operational smoke test: PASS")
