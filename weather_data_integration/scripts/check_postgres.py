import os
import psycopg

conninfo = os.getenv(
    "POSTGRES_CONNINFO",
    "host=localhost port=5432 dbname=weather user=weather_app password=weather_dev_password",
)

with psycopg.connect(conninfo) as conn:
    with conn.cursor() as cur:
        cur.execute("select version();")
        print(cur.fetchone()[0])
        cur.execute("select count(*) from weather_observation;")
        print("weather_observation rows:", cur.fetchone()[0])
