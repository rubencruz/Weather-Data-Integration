# Phase 2 — Data Engineering

## Goal

Turn the MVP into a small production-inspired pipeline.

## Notebook sequence

1. `01_bronze_ingestion.py`
2. `02_silver_transform.py`
3. `03_data_quality.py`
4. `04_gold_and_postgres.py`

## Engineering capabilities

- Bronze raw preservation.
- Silver schema normalization.
- Deduplication.
- Data quality validation.
- Quarantine for invalid data.
- Gold daily aggregations.
- Delta persistence.
- PostgreSQL publication.
- Environment-specific configuration.
- Job orchestration through a Declarative Automation Bundle.

## Next improvements

- Watermark/incremental ingestion.
- MERGE/upsert into PostgreSQL.
- Retry and backoff.
- Structured logging.
- Metrics and observability.
- Secret scopes.
- DEV/PROD catalogs/schemas.
- Integration tests against a disposable PostgreSQL container.
