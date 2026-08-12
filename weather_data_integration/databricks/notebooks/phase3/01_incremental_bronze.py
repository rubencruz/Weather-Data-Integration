# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 3 - Incremental Bronze ingestion
# MAGIC #
# MAGIC # Purpose: ingest Open-Meteo data repeatedly without creating duplicate source snapshots.
# MAGIC # Storage: Delta Lake / Unity Catalog managed table.

# COMMAND ----------

from datetime import datetime, timezone
from urllib.request import urlopen
from urllib.parse import urlencode
import json
from pyspark.sql import Row, functions as F
from delta.tables import DeltaTable

# Environment-specific values are supplied by the Databricks Job parameters.
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "weather_dev")
CATALOG = dbutils.widgets.get("catalog") or "main"
SCHEMA = dbutils.widgets.get("schema") if dbutils.widgets.get("schema") else "weather_dev"
BRONZE = f"{CATALOG}.{SCHEMA}.weather_bronze"

CITIES = [
    {"name": "Brasilia", "latitude": -15.793889, "longitude": -47.882778},
    {"name": "Sao Paulo", "latitude": -23.55052, "longitude": -46.633308},
    {"name": "Rio de Janeiro", "latitude": -22.906847, "longitude": -43.172896},
]

# The natural key identifies a source snapshot for a city and forecast generation time.
rows = []
for city in CITIES:
    params = urlencode({
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation",
        "forecast_days": 1,
        "timezone": "America/Sao_Paulo",
    })
    with urlopen(f"https://api.open-meteo.com/v1/forecast?{params}", timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))

    ingestion_ts = datetime.now(timezone.utc)
    rows.append(Row(
        city=city["name"],
        latitude=float(city["latitude"]),
        longitude=float(city["longitude"]),
        raw_payload=json.dumps(payload),
        source="open-meteo",
        ingestion_timestamp=ingestion_ts,
        ingestion_date=ingestion_ts.date().isoformat(),
    ))

incoming = spark.createDataFrame(rows)
incoming = incoming.withColumn(
    "snapshot_id",
    F.sha2(F.concat_ws("|", "city", "raw_payload"), 256)
)

# Create the Delta table if it does not exist.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE} (
    snapshot_id STRING,
    city STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    raw_payload STRING,
    source STRING,
    ingestion_timestamp TIMESTAMP,
    ingestion_date DATE
) USING DELTA
""")

# Idempotent append: only unseen snapshot_ids are inserted.
# 1. Verifica se a tabela já existe no banco de dados do Databricks
if spark.catalog.tableExists(BRONZE):
    # Se ela existe, carrega usando o método correto para nomes de tabelas
    target = DeltaTable.forName(spark, BRONZE)
    (
        target.alias("t")
        .merge(incoming.alias("s"), "t.snapshot_id = s.snapshot_id")
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    # Se a tabela não existir, faz a primeira carga direta criando a tabela
    incoming.write.format("delta").mode("append").saveAsTable(BRONZE)

print(f"Incoming snapshots: {incoming.count()}")
print(f"Bronze total rows: {spark.table(BRONZE).count()}")
display(spark.table(BRONZE).orderBy(F.col("ingestion_timestamp").desc()))
