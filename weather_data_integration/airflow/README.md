# Airflow — Phase 6

This directory contains the orchestration layer for the Weather Data Integration project.

Airflow does not run the Spark transformations locally. It triggers and monitors the Databricks job deployed by the existing Databricks Bundle.

## Start locally

```bash
docker compose up --build
```

Then configure:

- Airflow connection: `databricks_default`
- Airflow variable: `weather_databricks_job_id`

The DAG is:

```text
weather_data_integration_phase6
```
