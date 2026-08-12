from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def normalize_hourly_weather(raw_df: DataFrame) -> DataFrame:
    return (
        raw_df
        .select(
            "city",
            "latitude",
            "longitude",
            F.explode(
                F.arrays_zip(
                    "hourly.time",
                    "hourly.temperature_2m",
                    "hourly.relative_humidity_2m",
                    "hourly.wind_speed_10m",
                    "hourly.precipitation",
                )
            ).alias("x"),
        )
        .select(
            "city",
            "latitude",
            "longitude",
            F.to_timestamp(F.col("x.time")).alias("observation_time"),
            F.col("x.temperature_2m").cast("double").alias("temperature_c"),
            F.col("x.relative_humidity_2m").cast("double").alias("humidity_pct"),
            F.col("x.wind_speed_10m").cast("double").alias("wind_speed_kmh"),
            F.col("x.precipitation").cast("double").alias("precipitation_mm"),
        )
    )


def add_quality_columns(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn(
            "is_valid",
            F.col("city").isNotNull()
            & F.col("observation_time").isNotNull()
            & F.col("temperature_c").isNotNull()
            & F.col("humidity_pct").between(0, 100)
            & (F.col("wind_speed_kmh") >= 0)
            & (F.col("precipitation_mm") >= 0),
        )
        .withColumn("processed_at", F.current_timestamp())
    )


def daily_gold(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("city", F.to_date("observation_time").alias("observation_date"))
        .agg(
            F.avg("temperature_c").alias("avg_temperature_c"),
            F.min("temperature_c").alias("min_temperature_c"),
            F.max("temperature_c").alias("max_temperature_c"),
            F.avg("humidity_pct").alias("avg_humidity_pct"),
            F.sum("precipitation_mm").alias("total_precipitation_mm"),
            F.avg("wind_speed_kmh").alias("avg_wind_speed_kmh"),
            F.count("*").alias("record_count"),
        )
        .withColumn("processed_at", F.current_timestamp())
    )
