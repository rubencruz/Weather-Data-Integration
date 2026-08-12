# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 1 - Ingest Open-Meteo
# MAGIC
# MAGIC Fetch one day of hourly weather data for the configured cities.

# COMMAND ----------

from urllib.request import urlopen
from urllib.parse import urlencode
import json
from datetime import datetime, timezone

from pyspark.sql import Row

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

    rows.append({
        "city": city["name"],
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "hourly": payload["hourly"],
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
    })

raw_df = spark.createDataFrame(rows)
display(raw_df)
