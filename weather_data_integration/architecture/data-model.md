# Data model

## weather_observation

MVP target table.

| Column | Type | Description |
|---|---|---|
| city | varchar | City name |
| latitude | numeric | Latitude |
| longitude | numeric | Longitude |
| observation_time | timestamp | Observation time |
| temperature_c | numeric | Temperature in Celsius |
| humidity_pct | numeric | Relative humidity |
| wind_speed_kmh | numeric | Wind speed |
| precipitation_mm | numeric | Precipitation |
| ingestion_timestamp | timestamp | Pipeline ingestion time |

## weather_daily_gold

| Column | Type |
|---|---|
| city | varchar |
| observation_date | date |
| avg_temperature_c | numeric |
| min_temperature_c | numeric |
| max_temperature_c | numeric |
| avg_humidity_pct | numeric |
| total_precipitation_mm | numeric |
| avg_wind_speed_kmh | numeric |
| record_count | bigint |
| processed_at | timestamp |


## Phase 5 AI data products

```text
weather_silver
     |
weather_daily_gold
     |
weather_ai_features
     |
weather_ai_analysis
     |
weather_ai_insights
     |
weather_ai_quality
```

The AI Features layer is deterministic and contains model-ready context. Generative AI is isolated in the analysis stage and can be disabled without changing the downstream contracts.
