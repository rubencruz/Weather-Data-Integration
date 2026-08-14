# Phase 3 - Advanced Data Engineering

Phase 3 replaces the previous PostgreSQL publication target with **Delta Lake managed tables** and introduces production-oriented data engineering patterns.

## Capabilities

- Incremental Bronze ingestion
- Idempotent ingestion using `snapshot_id`
- Delta `MERGE` for Silver and Gold
- Medallion architecture
- Data Quality quarantine
- Quality metrics and quality gate
- Incremental Gold maintenance
- No dependency on an external database
- Unity Catalog managed Delta tables

## Tables

| Layer | Table | Purpose |
|---|---|---|
| Bronze | `main.weather_dev.weather_bronze` | Raw API snapshots |
| Silver | `main.weather_dev.weather_silver` | Normalized hourly observations |
| Quality | `main.weather_dev.weather_quarantine` | Invalid records |
| Quality | `main.weather_dev.weather_quality_metrics` | Quality history |
| Gold | `main.weather_dev.weather_daily_gold` | Daily analytics |

## Incremental strategy

1. API returns a new forecast snapshot.
2. `snapshot_id = SHA-256(city + raw_payload)` identifies the snapshot.
3. Bronze uses Delta `MERGE` and ignores an already-ingested snapshot.
4. Silver processes only snapshots not yet represented in Silver.
5. Silver uses `(city, observation_time)` as the observation business key.
6. Gold is upserted by `(city, observation_date)`.

## Quality gate

The Gold step fails when the latest quality score is below 100%. This is intentionally strict for the learning project and can later be changed to a configurable threshold.
