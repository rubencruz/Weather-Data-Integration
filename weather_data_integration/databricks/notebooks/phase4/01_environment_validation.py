# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 4 - Environment validation
# MAGIC #
# MAGIC # Confirms that the deployed job is using the intended catalog/schema and that Delta tables are available.

# COMMAND ----------

catalog = dbutils.widgets.get("catalog") if dbutils.widgets.get("catalog") else "main"
schema = dbutils.widgets.get("schema") or "weather_dev"

print(f"Environment schema: {catalog}.{schema}")
print("Spark version:", spark.version)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
print("Tables currently available:")
spark.sql(f"SHOW TABLES IN {catalog}.{schema}").show(truncate=False)
