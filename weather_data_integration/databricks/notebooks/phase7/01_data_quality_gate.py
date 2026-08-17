# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 7 - Data Quality Gate
# MAGIC #
# MAGIC # Converts the Phase 3 quality metrics into an explicit production gate.
# MAGIC # Downstream Gold/AI tasks are allowed to continue only when the latest
# MAGIC # quality score meets the configured threshold.

# COMMAND ----------

from pyspark.sql import functions as F
from uuid import uuid4


dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "weather_dev")
dbutils.widgets.text("min_quality_score", "0.95")

CATALOG = dbutils.widgets.get("catalog") or "main"
SCHEMA = dbutils.widgets.get("schema") or "weather_dev"
MIN_SCORE = float(dbutils.widgets.get("min_quality_score") or "0.95")
QUALITY = f"{CATALOG}.{SCHEMA}.weather_quality_metrics"
GATE = f"{CATALOG}.{SCHEMA}.weather_quality_gate"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GATE} (
    gate_run_id STRING,
    evaluated_at TIMESTAMP,
    quality_run_id STRING,
    quality_score DOUBLE,
    threshold DOUBLE,
    total_records BIGINT,
    valid_records BIGINT,
    invalid_records BIGINT,
    gate_status STRING
) USING DELTA
""")

latest = (
    spark.table(QUALITY)
    .orderBy(F.col("run_timestamp").desc())
    .limit(1)
)

if latest.limit(1).count() == 0:
    raise RuntimeError(f"No data quality metrics found in {QUALITY}")

row = latest.collect()[0]
score = float(row["quality_score"])
status = "PASS" if score >= MIN_SCORE else "FAIL"

gate_row = spark.createDataFrame([
    (
        str(uuid4()),
        row["run_timestamp"],
        row["quality_run_id"],
        score,
        MIN_SCORE,
        int(row["total_records"]),
        int(row["valid_records"]),
        int(row["invalid_records"]),
        status,
    )
], [
    "gate_run_id", "evaluated_at", "quality_run_id", "quality_score", "threshold",
    "total_records", "valid_records", "invalid_records", "gate_status"
]).withColumn("evaluated_at", F.current_timestamp())

gate_row.write.mode("append").format("delta").saveAsTable(GATE)

print(f"Quality score: {score:.2%}; threshold: {MIN_SCORE:.2%}; status: {status}")

if status != "PASS":
    raise RuntimeError(
        f"DATA_QUALITY_GATE_FAILED: score={score:.4f} is below threshold={MIN_SCORE:.4f}"
    )

