# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 2 - Bronze ingestion
# MAGIC Preserve the source payload and ingestion metadata in Delta.

# COMMAND ----------

from datetime import datetime, timezone
from urllib.request import urlopen
from urllib.parse import urlencode
import json

from pyspark.sql import Row

catalog = "main"
schema = "weather_dev"
bronze_table = f"{catalog}.{schema}.weather_bronze"

cities = [
    {"name": "Brasilia", "latitude": -15.793889, "longitude": -47.882778},
    {"name": "Sao Paulo", "latitude": -23.55052, "longitude": -46.633308},
    {"name": "Rio de Janeiro", "latitude": -22.906847, "longitude": -43.172896},
]

rows = []
for city in cities:
    params = urlencode({
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation",
        "forecast_days": 1,
        "timezone": "America/Sao_Paulo",
    })
    with urlopen(f"https://api.open-meteo.com/v1/forecast?{params}", timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))

    rows.append(Row(
        city=city["name"],
        latitude=float(city["latitude"]),
        longitude=float(city["longitude"]),
        raw_payload=json.dumps(payload),
        ingestion_timestamp=datetime.now(timezone.utc).isoformat(),
    ))

bronze_df = spark.createDataFrame(rows)
bronze_df.write.mode("append").format("delta").saveAsTable(bronze_table)

display(spark.table(bronze_table))
