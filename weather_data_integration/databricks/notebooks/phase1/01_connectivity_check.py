# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 1 - Connectivity Check
# MAGIC
# MAGIC Validate that the Databricks serverless environment can reach Open-Meteo.
# MAGIC PostgreSQL connectivity is checked only when a reachable JDBC endpoint is configured.

# COMMAND ----------

from urllib.request import urlopen
import json

url = "https://api.open-meteo.com/v1/forecast?latitude=-15.793889&longitude=-47.882778&current=temperature_2m"
with urlopen(url, timeout=20) as response:
    payload = json.loads(response.read().decode("utf-8"))

print("Open-Meteo OK:", payload.get("current", {}))
