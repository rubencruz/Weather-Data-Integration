# Architecture

## Phase 1 — MVP

```mermaid
flowchart LR
    A[Open-Meteo API] -->|HTTP GET JSON| B[Databricks Notebook]
    B --> C[PySpark DataFrame]
    C -->|JDBC| D[(PostgreSQL)]
```

## Phase 2 — Data Engineering

```mermaid
flowchart LR
    A[Open-Meteo API] --> B[Bronze Delta]
    B --> C[Silver Delta]
    C --> D{Data Quality}
    D -->|valid| E[Gold Delta]
    D -->|invalid| F[Quarantine Delta]
    E -->|JDBC| G[(PostgreSQL)]
```

## Logical data flow

```text
Source
  |
  | REST / JSON
  v
Bronze
  |
  | schema + normalization + deduplication
  v
Silver
  |
  +---- invalid ----> Quarantine
  |
  +---- valid ------> Gold
                          |
                          | JDBC
                          v
                      PostgreSQL
```

## Design principles

- Raw data is preserved before transformation.
- Transformations are deterministic and idempotent.
- Data quality failures do not silently disappear.
- Environment configuration is externalized.
- Secrets are never stored in Git.
- The same transformation code is intended for DEV and PROD.
