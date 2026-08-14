# Phase 1 — MVP

## Goal

Implement:

`Open-Meteo → Databricks/PySpark → PostgreSQL`

## Notebook sequence

1. `01_connectivity_check.py`
2. `02_ingest_open_meteo.py`
3. `03_transform_and_write.py`

## Acceptance criteria

- Open-Meteo request succeeds.
- At least 3 cities are ingested.
- Hourly records are normalized.
- PostgreSQL schema exists.
- JDBC write succeeds when a reachable target is configured.
- Unit tests pass locally.
