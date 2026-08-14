# Architecture

## Current architecture - Phases 3 to 5

```text
Open-Meteo REST API
        |
        v
Databricks Serverless / PySpark
        |
        v
+---------------------------+
| Bronze Delta              |
| raw snapshots / idempotent|
+-------------+-------------+
              |
              v
+---------------------------+
| Silver Delta              |
| typed observations        |
+-------------+-------------+
              |
       +------+------+
       |             |
       v             v
 Data Quality     AI Features
       |             |
       v             v
 Gold Delta      Anomaly Detection
                     |
                     v
               AI Insights Delta
                     |
             optional ai_query
                     |
                     v
               AI endpoint
```

## Storage

Delta Lake / Unity Catalog managed tables are the cloud system of record. PostgreSQL is retained only for optional local learning/testing and is not required by the Databricks Serverless pipeline.

## AI boundary

AI consumes trusted Silver/Gold-derived data and writes explicit Delta outputs. Generative AI is optional and does not become a dependency of ingestion, quality or core analytics.
